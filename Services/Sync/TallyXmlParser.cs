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

        public Acczite20.Models.Voucher ParseVoucherEntity(XElement vNode, Guid orgId, Guid companyId)
        {
            var now = DateTimeOffset.UtcNow;
            var voucher = new Acczite20.Models.Voucher
            {
                Id = Guid.NewGuid(),
                OrganizationId = orgId,
                CompanyId = companyId,
                TallyMasterId = vNode.Attribute("REMOTEID")?.Value 
                                ?? GetValue(vNode, "MASTERID")
                                ?? GetValue(vNode, "VOUCHERNUMBER")
                                ?? Guid.NewGuid().ToString(),
                VoucherNumber = GetValue(vNode, "VOUCHERNUMBER"),
                ReferenceNumber = GetValue(vNode, "REFERENCE"),
                Narration = GetValue(vNode, "NARRATION"),
                VoucherTypeId = Guid.Empty, // Mapper will resolve via VOUCHERTYPENAME if needed
                VoucherDate = DateTimeOffset.TryParseExact(GetValue(vNode, "DATE"), "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var d) ? d : DateTimeOffset.UtcNow,
                TotalAmount = 0,
                IsCancelled = GetValue(vNode, "ISCANCELLED").Equals("Yes", StringComparison.OrdinalIgnoreCase),
                IsOptional = GetValue(vNode, "ISOPTIONAL").Equals("Yes", StringComparison.OrdinalIgnoreCase),
                LastModified = now,
                CreatedAt = now,
                UpdatedAt = now,
                LedgerEntries = new List<Acczite20.Models.LedgerEntry>(),
                InventoryAllocations = new List<Acczite20.Models.InventoryAllocation>(),
                GstBreakdowns = new List<Acczite20.Models.GstBreakdown>()
            };

            if (string.IsNullOrWhiteSpace(voucher.ReferenceNumber))
            {
                voucher.ReferenceNumber = voucher.VoucherNumber;
            }

            // Capture Voucher Type Name for ID resolution
            var vTypeName = GetValue(vNode, "VOUCHERTYPENAME");
            if (string.IsNullOrEmpty(vTypeName)) vTypeName = "Journal";
            voucher.VoucherType = new Acczite20.Models.VoucherType { Name = vTypeName };

            // AlterId for incremental sync
            var alterIdStr = GetValue(vNode, "ALTERID");
            voucher.AlterId = int.TryParse(alterIdStr, out var aid) ? aid : 0;
            var inventoryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // 1. Ledger Entries
            var ledgerNodes = vNode.Descendants()
                .Where(x => x.Name.LocalName.Equals("ALLLEDGERENTRIES.LIST", StringComparison.OrdinalIgnoreCase) || 
                            x.Name.LocalName.Equals("LEDGERENTRIES.LIST", StringComparison.OrdinalIgnoreCase));

            foreach (var lNode in ledgerNodes)
            {
                var ledgerName = GetValue(lNode, "LEDGERNAME");
                if (string.IsNullOrWhiteSpace(ledgerName)) continue;

                var amountStr = GetValue(lNode, "AMOUNT");
                decimal amount = decimal.TryParse(amountStr, out var a) ? a : 0;

                // Tally Standard: Negative = Debit, Positive = Credit in vouchers
                decimal debit = amount < 0 ? Math.Abs(amount) : 0;
                decimal credit = amount > 0 ? amount : 0;

                var entry = new Acczite20.Models.LedgerEntry
                {
                    Id = Guid.NewGuid(),
                    OrganizationId = orgId,
                    VoucherId = voucher.Id,
                    LedgerName = ledgerName,
                    DebitAmount = debit,
                    CreditAmount = credit,
                    IsPartyLedger = GetValue(lNode, "ISPARTYLEDGER").Equals("Yes", StringComparison.OrdinalIgnoreCase)
                };
                voucher.LedgerEntries.Add(entry);

                // 2. Inventory Allocations
                foreach (var iNode in lNode.Descendants().Where(x => x.Name.LocalName.Equals("INVENTORYALLOCATIONS.LIST", StringComparison.OrdinalIgnoreCase)))
                {
                    var stockItem = GetValue(iNode, "STOCKITEMNAME");
                    if (!string.IsNullOrWhiteSpace(stockItem))
                    {
                        TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, orgId);
                    }
                }

                // 3. GST Breakdown
                if (ledgerName.Contains("GST", StringComparison.OrdinalIgnoreCase))
                {
                    var taxClassNodes = lNode.Descendants().Where(x => x.Name.LocalName.Equals("TAXCLASSIFICATIONDETAILS.LIST", StringComparison.OrdinalIgnoreCase));
                    foreach (var tx in taxClassNodes)
                    {
                        var taxType = GetValue(tx, "TAXCLASSIFICATIONNAME");
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            VoucherId = voucher.Id,
                            TaxType = string.IsNullOrEmpty(taxType) ? ledgerName : taxType,
                            AssessableValue = decimal.TryParse(GetValue(tx, "ASSESSABLEVALUE"), out var av) ? av : 0,
                            TaxRate = decimal.TryParse(GetValue(tx, "TAXRATE"), out var tr) ? tr : 0,
                            TaxAmount = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }

                    if (!taxClassNodes.Any()) 
                    {
                        voucher.GstBreakdowns.Add(new Acczite20.Models.GstBreakdown
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            VoucherId = voucher.Id,
                            TaxType = ledgerName,
                            TaxAmount = Math.Abs(credit > 0 ? credit : debit)
                        });
                    }
                }
            }

            // Some voucher shapes expose inventory at the voucher root rather than under ledger lines.
            foreach (var iNode in vNode.Descendants().Where(x =>
                         x.Name.LocalName.Equals("ALLINVENTORYENTRIES.LIST", StringComparison.OrdinalIgnoreCase) ||
                         x.Name.LocalName.Equals("INVENTORYENTRIES.LIST", StringComparison.OrdinalIgnoreCase)))
            {
                var stockItem = GetValue(iNode, "STOCKITEMNAME");
                if (!string.IsNullOrWhiteSpace(stockItem))
                {
                    TryAddInventoryAllocation(voucher, inventoryKeys, iNode, stockItem, orgId);
                }
            }

            // --- VOUCHER INTEGRITY CHECK (Golden Rule) ---
            decimal totalDebit = voucher.LedgerEntries.Sum(e => e.DebitAmount);
            decimal totalCredit = voucher.LedgerEntries.Sum(e => e.CreditAmount);

            if (Math.Abs(totalDebit - totalCredit) > 0.01m && !voucher.IsCancelled)
            {
                // Integrity Breach: This voucher is unbalanced. 
                // We return null to signal the Orchestrator to route this to the Dead Letter Queue.
                return null!; 
            }

            voucher.TotalAmount = totalCredit;
            return voucher;
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
