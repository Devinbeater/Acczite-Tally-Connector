using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace Acczite20.Services.Sync
{
    public class TallyXmlParser
    {
        public IEnumerable<XElement> ParseResponse(string xmlContent, string elementName)
        {
            if (string.IsNullOrWhiteSpace(xmlContent))
                return new List<XElement>();

            try
            {
                // Use non-strict XmlReader to ignore 0x04 and other control chars from Tally
                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var reader = new System.IO.StringReader(xmlContent);
                using var xmlReader = System.Xml.XmlReader.Create(reader, settings);
                var document = XDocument.Load(xmlReader);
                return document.Descendants(elementName);
            }
            catch { return new List<XElement>(); }
        }

        public MongoDB.Bson.BsonDocument ParseNodeToBson(XElement element)
        {
            var doc = new MongoDB.Bson.BsonDocument();
            foreach (var attr in element.Attributes())
            {
                doc[attr.Name.LocalName] = attr.Value;
            }
            foreach (var child in element.Elements())
            {
                if (!child.HasElements)
                {
                    doc[child.Name.LocalName] = child.Value;
                }
                else
                {
                    doc[child.Name.LocalName] = ParseNodeToBson(child);
                }
            }
            return doc;
        }

        private string GetValue(XElement parent, string tagName)
        {
            if (parent == null) return string.Empty;

            // Normalize Probe (Element + Attribute + Case)
            var val = parent.Element(tagName)?.Value 
                ?? parent.Attribute(tagName)?.Value 
                ?? parent.Element(tagName.ToUpper())?.Value 
                ?? parent.Attribute(tagName.ToUpper())?.Value
                ?? parent.Element(tagName.ToLower())?.Value;

            if (val != null) return val.Trim();

            // Scoped Deep Probe fallback (handles wrappers but avoids lists)
            return parent.Descendants()
                .Where(x => !x.Name.LocalName.EndsWith(".LIST", StringComparison.OrdinalIgnoreCase))
                .FirstOrDefault(x => x.Name.LocalName.Equals(tagName, StringComparison.OrdinalIgnoreCase))?.Value?.Trim() ?? string.Empty;
        }

        // ── Two-pass pipeline helpers ────────────────────────────────────────────
        //
        // Pass 1 — ParseVoucherHeader
        //   Called with AccziteVoucherHeaders response (scalar fields only).
        //   Returns a Voucher shell with empty collections and TotalAmount=0.
        //   Tally serialises ~1/10th the bytes of the old single-pass pipeline.
        //
        // Pass 2 — MergeVoucherDetail
        //   Called with AccziteVoucherDetail response (ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*).
        //   Populates LedgerEntries, InventoryAllocations, GstBreakdowns on the existing shell.
        //   Runs the double-entry integrity check and sets TotalAmount.
        //   Returns false when the voucher is unbalanced (caller routes to Dead Letter).
        //
        // ParseVoucherEntity — kept for backward compatibility; does both passes in one shot.

        public Acczite20.Models.Voucher? ParseVoucherHeader(XElement vNode, Guid orgId, Guid companyId)
        {
            var now = DateTimeOffset.UtcNow;
            var vTypeName = GetValue(vNode, "VOUCHERTYPENAME");
            if (string.IsNullOrEmpty(vTypeName)) vTypeName = "Journal";

            var voucher = new Acczite20.Models.Voucher
            {
                Id = Guid.NewGuid(),
                OrganizationId = orgId,
                CompanyId = companyId,
                TallyMasterId = vNode.Attribute("REMOTEID")?.Value
                                ?? GetValue(vNode, "MASTERID")
                                ?? GetValue(vNode, "VOUCHERNUMBER")
                                ?? Guid.NewGuid().ToString(),
                VoucherNumber  = GetValue(vNode, "VOUCHERNUMBER"),
                ReferenceNumber = GetValue(vNode, "REFERENCE"),
                Narration      = GetValue(vNode, "NARRATION"),
                VoucherTypeId  = Guid.Empty,
                VoucherDate    = DateTimeOffset.TryParseExact(GetValue(vNode, "DATE"), "yyyyMMdd", null,
                                     System.Globalization.DateTimeStyles.None, out var d) ? d : DateTimeOffset.UtcNow,
                TotalAmount    = 0,
                IsCancelled    = GetValue(vNode, "ISCANCELLED").Equals("Yes", StringComparison.OrdinalIgnoreCase),
                IsOptional     = GetValue(vNode, "ISOPTIONAL").Equals("Yes", StringComparison.OrdinalIgnoreCase),
                AlterId        = int.TryParse(GetValue(vNode, "ALTERID"), out var aid) ? aid : 0,
                LastModified   = now,
                CreatedAt      = now,
                UpdatedAt      = now,
                VoucherType    = new Acczite20.Models.VoucherType { Name = vTypeName },
                LedgerEntries        = new List<Acczite20.Models.LedgerEntry>(),
                InventoryAllocations = new List<Acczite20.Models.InventoryAllocation>(),
                GstBreakdowns        = new List<Acczite20.Models.GstBreakdown>()
            };

            if (string.IsNullOrWhiteSpace(voucher.ReferenceNumber))
                voucher.ReferenceNumber = voucher.VoucherNumber;

            return voucher;
        }

        // Returns true when the voucher passes the double-entry integrity check.
        // Returns false when unbalanced — caller should route to Dead Letter Queue.
        public bool MergeVoucherDetail(XElement vNode, Acczite20.Models.Voucher voucher)
        {
            var isCancelled = GetValue(vNode, "ISCANCELLED");
            if (!string.IsNullOrEmpty(isCancelled))
                voucher.IsCancelled = isCancelled.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var isOptional = GetValue(vNode, "ISOPTIONAL");
            if (!string.IsNullOrEmpty(isOptional))
                voucher.IsOptional = isOptional.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var refNum = GetValue(vNode, "REFERENCE");
            if (!string.IsNullOrEmpty(refNum))
                voucher.ReferenceNumber = refNum;

            var narration = GetValue(vNode, "NARRATION");
            if (!string.IsNullOrEmpty(narration))
                voucher.Narration = narration;

            var orgId = voucher.OrganizationId;
            var inventoryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // 1. Ledger entries (and anything nested under them)
            var ledgerNodes = vNode.Descendants()
                .Where(x => x.Name.LocalName.Equals("ALLLEDGERENTRIES.LIST", StringComparison.OrdinalIgnoreCase) ||
                            x.Name.LocalName.Equals("LEDGERENTRIES.LIST",    StringComparison.OrdinalIgnoreCase));

            foreach (var lNode in ledgerNodes)
            {
                var ledgerName = GetValue(lNode, "LEDGERNAME");
                if (string.IsNullOrWhiteSpace(ledgerName)) continue;

                decimal amount = decimal.TryParse(GetValue(lNode, "AMOUNT"), out var a) ? a : 0;
                decimal debit  = amount < 0 ? Math.Abs(amount) : 0;
                decimal credit = amount > 0 ? amount : 0;

                voucher.LedgerEntries.Add(new Acczite20.Models.LedgerEntry
                {
                    Id            = Guid.NewGuid(),
                    OrganizationId = orgId,
                    VoucherId     = voucher.Id,
                    LedgerName    = ledgerName,
                    DebitAmount   = debit,
                    CreditAmount  = credit,
                    IsPartyLedger = GetValue(lNode, "ISPARTYLEDGER").Equals("Yes", StringComparison.OrdinalIgnoreCase)
                });

                // Inventory allocations nested under ledger entries
                foreach (var iNode in lNode.Descendants().Where(x =>
                             x.Name.LocalName.Equals("INVENTORYALLOCATIONS.LIST", StringComparison.OrdinalIgnoreCase)))
                {
                    var stockItem = GetValue(iNode, "STOCKITEMNAME");
                    if (!string.IsNullOrWhiteSpace(stockItem))
                        TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, orgId);
                }

                // GST breakdowns nested under ledger entries
                if (ledgerName.Contains("GST", StringComparison.OrdinalIgnoreCase))
                {
                    var taxNodes = lNode.Descendants()
                        .Where(x => x.Name.LocalName.Equals("TAXCLASSIFICATIONDETAILS.LIST", StringComparison.OrdinalIgnoreCase))
                        .ToList();

                    foreach (var tx in taxNodes)
                    {
                        var taxType = GetValue(tx, "TAXCLASSIFICATIONNAME");
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id             = Guid.NewGuid(),
                            OrganizationId = orgId,
                            VoucherId      = voucher.Id,
                            TaxType        = string.IsNullOrEmpty(taxType) ? ledgerName : taxType,
                            AssessableValue = decimal.TryParse(GetValue(tx, "ASSESSABLEVALUE"), out var av) ? av : 0,
                            TaxRate        = decimal.TryParse(GetValue(tx, "TAXRATE"), out var tr) ? tr : 0,
                            TaxAmount      = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }

                    if (taxNodes.Count == 0)
                    {
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id             = Guid.NewGuid(),
                            OrganizationId = orgId,
                            VoucherId      = voucher.Id,
                            TaxType        = ledgerName,
                            TaxAmount      = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }
                }
            }

            // 2. Root-level inventory entries (non-accounting vouchers expose these at the VOUCHER level)
            foreach (var iNode in vNode.Descendants().Where(x =>
                         x.Name.LocalName.Equals("ALLINVENTORYENTRIES.LIST", StringComparison.OrdinalIgnoreCase) ||
                         x.Name.LocalName.Equals("INVENTORYENTRIES.LIST",    StringComparison.OrdinalIgnoreCase)))
            {
                var stockItem = GetValue(iNode, "STOCKITEMNAME");
                if (!string.IsNullOrWhiteSpace(stockItem))
                    TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, orgId);
            }

            // 3. Double-entry integrity check
            decimal totalDebit  = voucher.LedgerEntries.Sum(e => e.DebitAmount);
            decimal totalCredit = voucher.LedgerEntries.Sum(e => e.CreditAmount);

            if (Math.Abs(totalDebit - totalCredit) > 0.01m && !voucher.IsCancelled)
                return false; // caller routes to Dead Letter Queue

            voucher.TotalAmount = totalCredit;
            return true;
        }

        // ── 3-pass split helpers (used when at MinWindow with overload) ─────────────
        //
        // Pass 2a — MergeLedgerEntries
        //   Source: AccziteVoucherLedgers  (MASTERID, ALLLEDGERENTRIES.*)
        //   Processes: ledger entries + nested inventory + nested GST
        //   Runs the double-entry integrity check and sets TotalAmount.
        //   Returns false when unbalanced.
        //
        // Pass 2b — MergeInventoryEntries
        //   Source: AccziteVoucherInventory  (MASTERID, ALLINVENTORYENTRIES.*)
        //   Processes: root-level inventory entries only — no integrity check needed
        //   (accounting balance is entirely determined by ALLLEDGERENTRIES).

        public bool MergeLedgerEntries(XElement vNode, Acczite20.Models.Voucher voucher)
        {
            var isCancelled = GetValue(vNode, "ISCANCELLED");
            if (!string.IsNullOrEmpty(isCancelled))
                voucher.IsCancelled = isCancelled.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var isOptional = GetValue(vNode, "ISOPTIONAL");
            if (!string.IsNullOrEmpty(isOptional))
                voucher.IsOptional = isOptional.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var refNum = GetValue(vNode, "REFERENCE");
            if (!string.IsNullOrEmpty(refNum))
                voucher.ReferenceNumber = refNum;

            var narration = GetValue(vNode, "NARRATION");
            if (!string.IsNullOrEmpty(narration))
                voucher.Narration = narration;

            var orgId = voucher.OrganizationId;
            var inventoryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var ledgerNodes = vNode.Descendants()
                .Where(x => x.Name.LocalName.Equals("ALLLEDGERENTRIES.LIST", StringComparison.OrdinalIgnoreCase) ||
                            x.Name.LocalName.Equals("LEDGERENTRIES.LIST",    StringComparison.OrdinalIgnoreCase));

            foreach (var lNode in ledgerNodes)
            {
                var ledgerName = GetValue(lNode, "LEDGERNAME");
                if (string.IsNullOrWhiteSpace(ledgerName)) continue;

                decimal amount = decimal.TryParse(GetValue(lNode, "AMOUNT"), out var a) ? a : 0;
                decimal debit  = amount < 0 ? Math.Abs(amount) : 0;
                decimal credit = amount > 0 ? amount : 0;

                voucher.LedgerEntries.Add(new Acczite20.Models.LedgerEntry
                {
                    Id             = Guid.NewGuid(),
                    OrganizationId = orgId,
                    VoucherId      = voucher.Id,
                    LedgerName     = ledgerName,
                    DebitAmount    = debit,
                    CreditAmount   = credit,
                    IsPartyLedger  = GetValue(lNode, "ISPARTYLEDGER").Equals("Yes", StringComparison.OrdinalIgnoreCase)
                });

                foreach (var iNode in lNode.Descendants().Where(x =>
                             x.Name.LocalName.Equals("INVENTORYALLOCATIONS.LIST", StringComparison.OrdinalIgnoreCase)))
                {
                    var stockItem = GetValue(iNode, "STOCKITEMNAME");
                    if (!string.IsNullOrWhiteSpace(stockItem))
                        TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, orgId);
                }

                if (ledgerName.Contains("GST", StringComparison.OrdinalIgnoreCase))
                {
                    var taxNodes = lNode.Descendants()
                        .Where(x => x.Name.LocalName.Equals("TAXCLASSIFICATIONDETAILS.LIST", StringComparison.OrdinalIgnoreCase))
                        .ToList();

                    foreach (var tx in taxNodes)
                    {
                        var taxType = GetValue(tx, "TAXCLASSIFICATIONNAME");
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id              = Guid.NewGuid(),
                            OrganizationId  = orgId,
                            VoucherId       = voucher.Id,
                            TaxType         = string.IsNullOrEmpty(taxType) ? ledgerName : taxType,
                            AssessableValue = decimal.TryParse(GetValue(tx, "ASSESSABLEVALUE"), out var av) ? av : 0,
                            TaxRate         = decimal.TryParse(GetValue(tx, "TAXRATE"), out var tr) ? tr : 0,
                            TaxAmount       = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }

                    if (taxNodes.Count == 0)
                    {
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id             = Guid.NewGuid(),
                            OrganizationId = orgId,
                            VoucherId      = voucher.Id,
                            TaxType        = ledgerName,
                            TaxAmount      = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }
                }
            }

            decimal totalDebit  = voucher.LedgerEntries.Sum(e => e.DebitAmount);
            decimal totalCredit = voucher.LedgerEntries.Sum(e => e.CreditAmount);
            if (Math.Abs(totalDebit - totalCredit) > 0.01m && !voucher.IsCancelled)
                return false;

            voucher.TotalAmount = totalCredit;
            return true;
        }

        // No integrity check — root-level inventory does not affect accounting balance.
        public void MergeInventoryEntries(XElement vNode, Acczite20.Models.Voucher voucher)
        {
            var isCancelled = GetValue(vNode, "ISCANCELLED");
            if (!string.IsNullOrEmpty(isCancelled))
                voucher.IsCancelled = isCancelled.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var isOptional = GetValue(vNode, "ISOPTIONAL");
            if (!string.IsNullOrEmpty(isOptional))
                voucher.IsOptional = isOptional.Equals("Yes", StringComparison.OrdinalIgnoreCase);

            var refNum = GetValue(vNode, "REFERENCE");
            if (!string.IsNullOrEmpty(refNum))
                voucher.ReferenceNumber = refNum;

            var narration = GetValue(vNode, "NARRATION");
            if (!string.IsNullOrEmpty(narration))
                voucher.Narration = narration;

            var inventoryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var iNode in vNode.Descendants().Where(x =>
                         x.Name.LocalName.Equals("ALLINVENTORYENTRIES.LIST", StringComparison.OrdinalIgnoreCase) ||
                         x.Name.LocalName.Equals("INVENTORYENTRIES.LIST",    StringComparison.OrdinalIgnoreCase)))
            {
                var stockItem = GetValue(iNode, "STOCKITEMNAME");
                if (!string.IsNullOrWhiteSpace(stockItem))
                    TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, voucher.OrganizationId);
            }
        }

        // Single-pass convenience wrapper — used by any code path that still provides
        // a full VOUCHER element containing both header fields and ledger detail.
        public Acczite20.Models.Voucher? ParseVoucherEntity(XElement vNode, Guid orgId, Guid companyId)
        {
            var voucher = ParseVoucherHeader(vNode, orgId, companyId);
            if (voucher == null) return null;
            return MergeVoucherDetail(vNode, voucher) ? voucher : null;
        }

        private void TryAddInventoryAllocation(
            Acczite20.Models.Voucher voucher,
            HashSet<string> inventoryKeys,
            XElement inventoryNode,
            string stockItem,
            Guid orgId)
        {
            var actualQuantity = ParseLeadingDecimal(GetValue(inventoryNode, "ACTUALQTY"));
            var billedQuantity = ParseLeadingDecimal(GetValue(inventoryNode, "BILLEDQTY"));
            var rate = ParseRate(GetValue(inventoryNode, "RATE"));
            var amount = decimal.TryParse(GetValue(inventoryNode, "AMOUNT"), out var parsedAmount) ? Math.Abs(parsedAmount) : 0;

            var key = $"{stockItem}|{actualQuantity}|{billedQuantity}|{rate}|{amount}";
            if (!inventoryKeys.Add(key))
            {
                return;
            }

            voucher.InventoryAllocations.Add(new Acczite20.Models.InventoryAllocation
            {
                Id = Guid.NewGuid(),
                OrganizationId = orgId,
                VoucherId = voucher.Id,
                StockItemName = stockItem,
                ActualQuantity = actualQuantity,
                BilledQuantity = billedQuantity,
                Rate = rate,
                Amount = amount
            });
        }

        private static decimal ParseLeadingDecimal(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return 0;
            }

            var token = value.Split(' ')[0];
            return decimal.TryParse(token, out var parsed) ? parsed : 0;
        }

        private static decimal ParseRate(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return 0;
            }

            var token = value.Split('/')[0];
            return decimal.TryParse(token, out var parsed) ? parsed : 0;
        }
    }
}
