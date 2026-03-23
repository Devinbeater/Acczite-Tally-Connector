-- ==========================================================
-- Enterprise Tally BI: Partitioning & Compression Script
-- Target: SQL Server
-- ==========================================================

-- 1. Create Partition Function (Annual Partitioning)
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pfVoucherDate')
BEGIN
    CREATE PARTITION FUNCTION pfVoucherDate (DATETIMEOFFSET)
    AS RANGE RIGHT FOR VALUES 
    (
        '2023-01-01', 
        '2024-01-01', 
        '2025-01-01', 
        '2026-01-01', 
        '2027-01-01', 
        '2028-01-01', 
        '2029-01-01', 
        '2030-01-01'
    );
END
GO

-- 2. Create Partition Scheme
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'psVoucherDate')
BEGIN
    CREATE PARTITION SCHEME psVoucherDate
    AS PARTITION pfVoucherDate
    ALL TO ([PRIMARY]);
END
GO

-- 3. Apply Partitioning to FactVouchers
-- Note: Requires rebuilding the table or creating it partitioned from start.
-- Since this is a new warehouse setup, we define the partitioned tables.

/*
ALTER TABLE FactVouchers
REBUILD WITH (PARTITION = ALL, DATA_COMPRESSION = ROW);

ALTER TABLE FactLedgerEntries
REBUILD WITH (PARTITION = ALL, DATA_COMPRESSION = ROW);
*/

-- 4. Enable Row Compression on All Warehouse Tables (Mistake 6 & 10 Mitigation)
ALTER TABLE FactVouchers REBUILD WITH (DATA_COMPRESSION = ROW);
ALTER TABLE FactLedgerEntries REBUILD WITH (DATA_COMPRESSION = ROW);
ALTER TABLE FactInventoryMovements REBUILD WITH (DATA_COMPRESSION = ROW);
ALTER TABLE FactNarrations REBUILD WITH (DATA_COMPRESSION = ROW);
ALTER TABLE LedgerBalanceSnapshots REBUILD WITH (DATA_COMPRESSION = ROW);

-- 5. Periodic Index Optimization (Recommended for BI)
-- CREATE COLUMNSTORE INDEX CSI_FactLedgerEntries ON FactLedgerEntries(LedgerId, VoucherDate, Debit, Credit) 
-- WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0);
GO
