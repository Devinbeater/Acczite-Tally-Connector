-- ============================================================
-- Acczite20 — Fact Table Reset & Index Script
-- Run this ONCE after the Tally sign-convention fix
-- (positive AMOUNT = Debit, negative AMOUNT = Credit)
-- ============================================================

-- ── STEP 1: Apply missing warehouse indexes ──────────────────
-- Safe to run on existing schema — uses IF NOT EXISTS guards.

-- Covering index: period-totals JOIN + ledger drilldown
-- Satisfies: WHERE (OrganizationId, VoucherDate) + reads Debit/Credit/LedgerId
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactLedgerEntries')
      AND name = 'IX_FactLedgerEntries_OrgDate_Covering'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactLedgerEntries_OrgDate_Covering
    ON FactLedgerEntries (OrganizationId, VoucherDate, VoucherId)
    INCLUDE (Debit, Credit, LedgerId);
    PRINT 'Created IX_FactLedgerEntries_OrgDate_Covering';
END

-- Per-ledger drilldown (WHERE OrganizationId, LedgerId range scan on VoucherDate)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactLedgerEntries')
      AND name = 'IX_FactLedgerEntries_OrgLedgerDate'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactLedgerEntries_OrgLedgerDate
    ON FactLedgerEntries (OrganizationId, LedgerId, VoucherDate);
    PRINT 'Created IX_FactLedgerEntries_OrgLedgerDate';
END

-- Paging + ORDER BY on FactVouchers
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactVouchers')
      AND name = 'IX_FactVouchers_OrgDateNumber'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_FactVouchers_OrgDateNumber
    ON FactVouchers (OrganizationId, VoucherDate, VoucherNumber);
    PRINT 'Created IX_FactVouchers_OrgDateNumber';
END

-- Unique: exactly one narration per voucher (prevents duplicate rows in Daybook Query 1)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('FactNarrations')
      AND name = 'UX_FactNarrations_VoucherId'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_FactNarrations_VoucherId
    ON FactNarrations (VoucherId);
    PRINT 'Created UX_FactNarrations_VoucherId';
END

GO

-- ── STEP 2: Sanity-check sign state before touching data ─────
-- Run this SELECT first. If TotalDr > 0 and TotalCr = 0 for Sales ledgers,
-- all existing data was written by the OLD (wrong) parser → safe to use Option A.
-- If TotalCr > 0, rows were already written by the FIXED parser → use Option B.

SELECT
    l.Name            AS LedgerName,
    l.ParentGroup     AS [Group],
    SUM(fle.Debit)    AS TotalDr,
    SUM(fle.Credit)   AS TotalCr
FROM FactLedgerEntries fle
JOIN Ledgers l ON fle.LedgerId = l.Id
WHERE l.ParentGroup IN (
    'Sales Accounts', 'Direct Incomes', 'Indirect Incomes',
    'Direct Expenses', 'Indirect Expenses', 'Purchase Accounts'
)
GROUP BY l.Name, l.ParentGroup
ORDER BY l.ParentGroup, l.Name;

GO

-- ── STEP 3A: Option A — FULL RESET (Recommended) ────────────
-- Wipes all fact data. Re-sync from Tally after this.
-- Uncomment and run after confirming you can re-sync.

/*
DELETE FROM FactNarrations;
DELETE FROM FactLedgerEntries;
DELETE FROM FactInventoryMovements;
DELETE FROM FactVouchers;
PRINT 'Fact tables cleared. Run full Tally re-sync.';
*/

-- ── STEP 3B: Option B — IN-PLACE SWAP (only if 100% old data) ─
-- Use ONLY when the sanity check (Step 2) shows TotalDr>0 and TotalCr=0
-- for ALL income ledgers (meaning every row was from the old parser).
-- DO NOT run if even one row has already been written by the fixed parser.

/*
UPDATE FactLedgerEntries
SET
    Debit  = Credit,
    Credit = Debit;
PRINT 'Dr/Cr columns swapped. Verify Trial Balance immediately.';
*/

-- ── STEP 4: Post-sync validation suite ──────────────────────
-- Run ALL four checks after re-sync. Every check must pass.

-- 4A: Overall TB balance — Dr MUST equal Cr
SELECT
    SUM(Debit)  AS TotalDr,
    SUM(Credit) AS TotalCr,
    CASE WHEN ABS(SUM(Debit) - SUM(Credit)) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS BalanceCheck
FROM FactLedgerEntries;

-- 4B: Sign check by group — Income must be Credit, Expenses must be Debit
SELECT
    l.ParentGroup                   AS [Group],
    SUM(fle.Debit)                  AS TotalDr,
    SUM(fle.Credit)                 AS TotalCr,
    CASE
        WHEN l.ParentGroup IN ('Sales Accounts','Direct Incomes','Indirect Incomes')
             AND SUM(fle.Credit) > 0 AND SUM(fle.Debit) = 0 THEN 'PASS'
        WHEN l.ParentGroup IN ('Direct Expenses','Indirect Expenses','Purchase Accounts')
             AND SUM(fle.Debit) > 0 AND SUM(fle.Credit) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                             AS SignCheck
FROM FactLedgerEntries fle
JOIN Ledgers l ON fle.LedgerId = l.Id
WHERE l.ParentGroup IN (
    'Sales Accounts','Direct Incomes','Indirect Incomes',
    'Direct Expenses','Indirect Expenses','Purchase Accounts'
)
GROUP BY l.ParentGroup
ORDER BY l.ParentGroup;

-- 4C: Voucher integrity — every voucher must have Dr == Cr
-- Result MUST be 0 rows. Any row here = corrupt voucher in DB.
SELECT
    fv.Id           AS VoucherId,
    fv.VoucherNumber,
    SUM(fle.Debit)  AS VoucherDr,
    SUM(fle.Credit) AS VoucherCr,
    ABS(SUM(fle.Debit) - SUM(fle.Credit)) AS Imbalance
FROM FactLedgerEntries fle
JOIN FactVouchers fv ON fle.VoucherId = fv.Id
WHERE fv.IsCancelled = 0 AND fv.IsOptional = 0
GROUP BY fv.Id, fv.VoucherNumber
HAVING ABS(SUM(fle.Debit) - SUM(fle.Credit)) > 0.01
ORDER BY Imbalance DESC;

-- 4D: DLQ health — count of failed vouchers not yet replayed
-- Should be 0 after a clean sync. Non-zero = investigate immediately.
SELECT COUNT(*) AS DeadLetterCount FROM DeadLetters WHERE IsReplayed = 0;
