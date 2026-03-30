using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Acczite20.Models.Warehouse;
using Acczite20.Services;
using MySqlConnector;
using Acczite20.Models;
using Acczite20.Data;
using Microsoft.EntityFrameworkCore;

using MongoDB.Bson;

namespace Acczite20.Infrastructure
{
    public class BulkInsertHandler
    {
        private readonly AppDbContext _dbContext;
        private readonly MasterDataCache _cache;
        private readonly Services.Sync.LedgerSnapshotService _snapshotService;
        private readonly Services.MongoService _mongoService;

        public BulkInsertHandler(
            AppDbContext dbContext, 
            MasterDataCache cache, 
            Services.Sync.LedgerSnapshotService snapshotService,
            Services.MongoService mongoService)
        {
            _dbContext = dbContext;
            _cache = cache;
            _snapshotService = snapshotService;
            _mongoService = mongoService;
        }

        public async Task BulkInsertVouchersAsync(List<Voucher> vouchers)
        {
            if (vouchers == null || !vouchers.Any()) return;

            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            var connectionString = _dbContext.Database.GetConnectionString();

            if (dbType == "SQL Server") 
            {
                await SqlBulkInsertAsync(vouchers, connectionString);
                await SqlBulkInsertWarehouseAsync(vouchers, connectionString);
            }
            else if (dbType == "MySQL") 
            {
                await MySqlBulkInsertAsync(vouchers, connectionString);
            }
            else if (dbType == "MongoDB")
            {
                await MongoBulkInsertVouchersAsync(vouchers);
                // Also keep in EF for in-memory cache if needed by other parts of the app
                await EFBulkInsertAsync(vouchers);
            }
            else 
            {
                await EFBulkInsertAsync(vouchers);
            }

            // Notify Timeline
            await Acczite20.Core.Events.EventBus.PublishAsync(new Acczite20.Core.Events.BatchSyncCompletedEvent("Vouchers", vouchers.Count));
        }

        private async Task MongoBulkInsertVouchersAsync(List<Voucher> vouchers)
        {
            var docs = vouchers.Select(v => {
                var doc = new BsonDocument
                {
                    { "voucherNo", v.VoucherNumber },
                    { "type", v.VoucherType?.Name ?? "Journal" },
                    { "date", v.VoucherDate.UtcDateTime },
                    { "totalAmount", (double)v.TotalAmount },
                    { "narration", v.Narration ?? "" },
                    { "TallyMasterId", v.TallyMasterId ?? Guid.NewGuid().ToString() },
                    { "SyncRunId", v.SyncRunId?.ToString() },
                    { "isCancelled", v.IsCancelled }
                };

                if (v.LedgerEntries != null && v.LedgerEntries.Any())
                {
                    var entries = new BsonArray();
                    foreach (var le in v.LedgerEntries)
                    {
                        entries.Add(new BsonDocument {
                            { "ledgerName", le.LedgerName },
                            { "debit", (double)le.DebitAmount },
                            { "credit", (double)le.CreditAmount }
                        });
                    }
                    doc["ledgerEntries"] = entries;
                }

                if (v.InventoryAllocations != null && v.InventoryAllocations.Any())
                {
                    var items = new BsonArray();
                    foreach (var ia in v.InventoryAllocations)
                    {
                        items.Add(new BsonDocument {
                            { "stockItemName", ia.StockItemName },
                            { "quantity", (double)ia.BilledQuantity },
                            { "rate", (double)ia.Rate },
                            { "amount", (double)ia.Amount }
                        });
                    }
                    doc["inventoryEntries"] = items;
                }

                if (v.BillAllocations != null && v.BillAllocations.Any())
                {
                    var bills = new BsonArray();
                    foreach (var ba in v.BillAllocations)
                    {
                        bills.Add(new BsonDocument {
                            { "billName", ba.BillName },
                            { "billType", ba.BillType },
                            { "amount", (double)ba.Amount }
                        });
                    }
                    doc["billAllocations"] = bills;
                }

                return doc;
            });

            await _mongoService.BulkUpsertDocumentsAsync("vouchers", docs, "TallyMasterId");
        }

        private async Task SqlBulkInsertWarehouseAsync(List<FactVoucher> factVouchers, List<FactLedgerEntry> factEntries, List<FactInventoryMovement> factMovements, List<FactNarration> factNarrations, string connectionString)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                DataTable fvTable = GetFactVoucherDataTable(factVouchers);
                DataTable feTable = GetFactLedgerEntryDataTable(factEntries);
                DataTable fmTable = GetFactInventoryMovementDataTable(factMovements);
                DataTable fnTable = GetFactNarrationDataTable(factNarrations);

                using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                async Task Copy(DataTable dt, string dest)
                {
                    bulkCopy.DestinationTableName = dest;
                    bulkCopy.ColumnMappings.Clear();
                    foreach (DataColumn c in dt.Columns) bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
                    await bulkCopy.WriteToServerAsync(dt);
                }

                await Copy(fvTable, "FactVouchers");
                await Copy(feTable, "FactLedgerEntries");
                await Copy(fmTable, "FactInventoryMovements");
                await Copy(fnTable, "FactNarrations");

                await transaction.CommitAsync();

                // Update Reporting Snapshots (Materialized Layer)
                await _snapshotService.UpdateSnapshotsAsync(SessionManager.Instance.OrganizationId, factEntries.Where(e => e.LedgerId != Guid.Empty).ToList());
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        private async Task SqlBulkInsertWarehouseAsync(List<Voucher> vouchers, string connectionString)
        {
            // Project Raw Vouchers to Fact Models
            var factVouchers = vouchers.Select(v => new FactVoucher
            {
                Id = Guid.NewGuid(),
                OrganizationId = v.OrganizationId,
                CompanyId = v.CompanyId,
                VoucherNumber = v.VoucherNumber,
                VoucherTypeId = _cache.GetVoucherTypeId(v.VoucherType?.Name ?? ""),
                VoucherDate = v.VoucherDate,
                TotalAmount = v.TotalAmount,
                TallyMasterId = v.TallyMasterId,
                AlterId = v.AlterId,
                IsCancelled = v.IsCancelled,
                IsOptional = v.IsOptional,
                CreatedAt = DateTimeOffset.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow
            }).ToList();

            var factEntries = vouchers.SelectMany(v => v.LedgerEntries.Select(le => new FactLedgerEntry
            {
                Id = Guid.NewGuid(),
                OrganizationId = v.OrganizationId,
                VoucherId = factVouchers.First(fv => fv.VoucherNumber == v.VoucherNumber).Id,
                LedgerId = _cache.GetLedgerId(le.LedgerName),
                Debit = le.DebitAmount,
                Credit = le.CreditAmount,
                VoucherDate = v.VoucherDate,
                CreatedAt = DateTimeOffset.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow
            })).ToList();

            var factMovements = vouchers.SelectMany(v => v.InventoryAllocations.Select(ia => new FactInventoryMovement
            {
                Id = Guid.NewGuid(),
                OrganizationId = v.OrganizationId,
                VoucherId = factVouchers.First(fv => fv.VoucherNumber == v.VoucherNumber).Id,
                StockItemId = _cache.GetStockItemId(ia.StockItemName),
                Quantity = ia.BilledQuantity,
                Rate = ia.Rate,
                Amount = ia.Amount,
                VoucherDate = v.VoucherDate,
                CreatedAt = DateTimeOffset.UtcNow,
                UpdatedAt = DateTimeOffset.UtcNow
            })).ToList();

            var factNarrations = vouchers
                .Where(v => !string.IsNullOrEmpty(v.Narration))
                .Select(v => new FactNarration
                {
                    Id = Guid.NewGuid(),
                    OrganizationId = v.OrganizationId,
                    VoucherId = factVouchers.First(fv => fv.VoucherNumber == v.VoucherNumber).Id,
                    Narration = v.Narration,
                    CreatedAt = DateTimeOffset.UtcNow,
                    UpdatedAt = DateTimeOffset.UtcNow
                }).ToList();
            
            await SqlBulkInsertWarehouseAsync(factVouchers, factEntries, factMovements, factNarrations, connectionString);
        }

        private async Task SqlBulkInsertAsync(List<Voucher> vouchers, string connectionString)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                // To ensure idempotency (prevent duplicates), we delete any existing vouchers
                // that match the TallyMasterIds we are about to insert.
                await EnsureIdempotencyAsync(vouchers, connection, transaction);

                // Create DataTables for Vouchers and their Child Collections
                DataTable voucherTable = GetVoucherDataTable(vouchers);
                DataTable ledgerTable = GetLedgerEntryDataTable(vouchers.SelectMany(v => v.LedgerEntries).ToList());
                DataTable inventoryTable = GetInventoryAllocationsTable(vouchers.SelectMany(v => v.InventoryAllocations).ToList());
                DataTable gstTable = GetGstBreakdownDataTable(vouchers.SelectMany(v => v.GstBreakdowns).ToList());
                DataTable billTable = GetBillAllocationsTable(vouchers.SelectMany(v => v.BillAllocations).ToList());

                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    async Task PerformBulkCopy(DataTable table, string destination)
                    {
                        bulkCopy.DestinationTableName = destination;
                        bulkCopy.ColumnMappings.Clear();
                        foreach (DataColumn column in table.Columns)
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        await bulkCopy.WriteToServerAsync(table);
                    }

                    await PerformBulkCopy(voucherTable, "Vouchers");
                    await PerformBulkCopy(ledgerTable, "LedgerEntries");
                    await PerformBulkCopy(inventoryTable, "InventoryAllocations");
                    await PerformBulkCopy(gstTable, "GstBreakdowns");
                    await PerformBulkCopy(billTable, "BillAllocations");
                }

                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        private async Task EnsureIdempotencyAsync(List<Voucher> vouchers, SqlConnection conn, SqlTransaction trans)
        {
            var masterIds = vouchers.Where(v => !string.IsNullOrEmpty(v.TallyMasterId))
                                   .Select(v => v.TallyMasterId).ToList();
            if (!masterIds.Any()) return;

            // In SQL Server, we can use a temporary table to efficiently delete duplicates
            var tempTableName = $"#VoucherIds_{Guid.NewGuid():N}";
            using (var cmd = new SqlCommand($"CREATE TABLE {tempTableName} (MasterId NVARCHAR(100))", conn, trans))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, trans))
            {
                bulkCopy.DestinationTableName = tempTableName;
                var idTable = new DataTable();
                idTable.Columns.Add("MasterId", typeof(string));
                foreach (var id in masterIds) idTable.Rows.Add(id);
                await bulkCopy.WriteToServerAsync(idTable);
            }

            // Delete existing records (Cascade delete should handle children if configured, 
            // otherwise we delete children manually based on VoucherId)
            var deleteSql = $@"
                DELETE FROM GstBreakdowns WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM {tempTableName}));
                DELETE FROM BillAllocations WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM {tempTableName}));
                DELETE FROM InventoryAllocations WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM {tempTableName}));
                DELETE FROM LedgerEntries WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM {tempTableName}));
                DELETE FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM {tempTableName});
            ";

            using (var cmd = new SqlCommand(deleteSql, conn, trans))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private async Task MySqlBulkInsertAsync(List<Voucher> vouchers, string connectionString)
        {
            using var connection = new MySqlConnector.MySqlConnection(connectionString);
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            try
            {
                // Ensure Idempotency for MySQL
                await MySqlEnsureIdempotencyAsync(vouchers, connection, transaction);
                
                // MySQL doesn't have a direct equivalent to SqlBulkCopy that's as easy as DataTable,
                // so we use EF but within the shared transaction for atomicity.
                _dbContext.Database.UseTransaction(transaction);
                await EFBulkInsertAsync(vouchers);
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        private async Task MySqlEnsureIdempotencyAsync(List<Voucher> vouchers, MySqlConnector.MySqlConnection conn, MySqlConnector.MySqlTransaction trans)
        {
            var masterIds = vouchers.Where(v => !string.IsNullOrEmpty(v.TallyMasterId))
                                   .Select(v => v.TallyMasterId).ToList();
            if (!masterIds.Any()) return;

            // MySQL approach using a temporary table
            var tempTableName = $"VoucherIds_{Guid.NewGuid():N}";
            var createTable = $"CREATE TEMPORARY TABLE `{tempTableName}` (MasterId VARCHAR(100))";
            using (var cmd = new MySqlConnector.MySqlCommand(createTable, conn, trans))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            // Insert IDs into temp table using a batch insert
            var insertSql = $"INSERT INTO `{tempTableName}` (MasterId) VALUES (@id)";
            using (var cmd = new MySqlConnector.MySqlCommand(insertSql, conn, trans))
            {
                cmd.Parameters.Add("@id", MySqlConnector.MySqlDbType.VarChar);
                foreach (var id in masterIds)
                {
                    cmd.Parameters["@id"].Value = id;
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            // Delete existing records (Explicitly handle children if FKs are not ON DELETE CASCADE)
            var deleteSql = $@"
                DELETE FROM GstBreakdowns WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM `{tempTableName}`));
                DELETE FROM BillAllocations WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM `{tempTableName}`));
                DELETE FROM InventoryAllocations WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM `{tempTableName}`));
                DELETE FROM LedgerEntries WHERE VoucherId IN (SELECT Id FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM `{tempTableName}`));
                DELETE FROM Vouchers WHERE TallyMasterId IN (SELECT MasterId FROM `{tempTableName}`);
            ";

            using (var cmd = new MySqlConnector.MySqlCommand(deleteSql, conn, trans))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private async Task EFBulkInsertAsync(List<Voucher> vouchers)
        {
            _dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
            await _dbContext.Vouchers.AddRangeAsync(vouchers);
            await _dbContext.SaveChangesAsync();
            _dbContext.ChangeTracker.Clear();
        }

        #region DataTable Helpers
        private DataTable GetVoucherDataTable(List<Voucher> vouchers)
        {
            var table = new DataTable("Vouchers");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("CompanyId", typeof(Guid));
            table.Columns.Add("VoucherNumber", typeof(string));
            table.Columns.Add("VoucherTypeId", typeof(Guid));
            table.Columns.Add("VoucherDate", typeof(DateTimeOffset));
            table.Columns.Add("ReferenceNumber", typeof(string));
            table.Columns.Add("IsCancelled", typeof(bool));
            table.Columns.Add("IsOptional", typeof(bool));
            table.Columns.Add("TallyMasterId", typeof(string));
            table.Columns.Add("TotalAmount", typeof(decimal));
            table.Columns.Add("LastModified", typeof(DateTimeOffset));
            table.Columns.Add("CreatedAt", typeof(DateTimeOffset));
            table.Columns.Add("UpdatedAt", typeof(DateTimeOffset));
            table.Columns.Add("SyncRunId", typeof(Guid));

            foreach (var v in vouchers)
            {
                table.Rows.Add(v.Id, v.OrganizationId, v.CompanyId, v.VoucherNumber, v.VoucherTypeId, v.VoucherDate, v.ReferenceNumber, v.IsCancelled, v.IsOptional, v.TallyMasterId, v.TotalAmount, v.LastModified, v.CreatedAt, v.UpdatedAt, v.SyncRunId);
            }
            return table;
        }

        private DataTable GetLedgerEntryDataTable(List<LedgerEntry> entries)
        {
            var table = new DataTable("LedgerEntries");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("LedgerName", typeof(string));
            table.Columns.Add("DebitAmount", typeof(decimal));
            table.Columns.Add("CreditAmount", typeof(decimal));
            foreach (var e in entries) table.Rows.Add(e.Id, e.OrganizationId, e.VoucherId, e.LedgerName, e.DebitAmount, e.CreditAmount);
            return table;
        }

        private DataTable GetInventoryAllocationsTable(List<InventoryAllocation> items)
        {
            var table = new DataTable("InventoryAllocations");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("StockItemName", typeof(string));
            table.Columns.Add("ActualQuantity", typeof(decimal));
            table.Columns.Add("BilledQuantity", typeof(decimal));
            table.Columns.Add("Rate", typeof(decimal));
            table.Columns.Add("Amount", typeof(decimal));

            foreach (var i in items)
            {
                table.Rows.Add(i.Id, i.OrganizationId, i.VoucherId, i.StockItemName, i.ActualQuantity, i.BilledQuantity, i.Rate, i.Amount);
            }
            return table;
        }

        private DataTable GetGstBreakdownDataTable(List<GstBreakdown> lines)
        {
            var table = new DataTable("GstBreakdowns");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("GstType", typeof(string));
            table.Columns.Add("Amount", typeof(decimal));
            foreach (var g in lines) table.Rows.Add(g.Id, g.OrganizationId, g.VoucherId, g.TaxType, g.TaxAmount);
            return table;
        }
        private DataTable GetFactVoucherDataTable(List<FactVoucher> vouchers)
        {
            var table = new DataTable("FactVouchers");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("CompanyId", typeof(Guid));
            table.Columns.Add("VoucherNumber", typeof(string));
            table.Columns.Add("VoucherTypeId", typeof(Guid));
            table.Columns.Add("VoucherDate", typeof(DateTimeOffset));
            table.Columns.Add("TotalAmount", typeof(decimal));
            table.Columns.Add("AlterId", typeof(int));
            table.Columns.Add("IsCancelled", typeof(bool));
            table.Columns.Add("IsOptional", typeof(bool));
            table.Columns.Add("CreatedAt", typeof(DateTimeOffset));
            table.Columns.Add("UpdatedAt", typeof(DateTimeOffset));
            foreach (var v in vouchers) table.Rows.Add(v.Id, v.OrganizationId, v.CompanyId, v.VoucherNumber, v.VoucherTypeId, v.VoucherDate, v.TotalAmount, v.AlterId, v.IsCancelled, v.IsOptional, v.CreatedAt, v.UpdatedAt);
            return table;
        }

        private DataTable GetFactNarrationDataTable(List<FactNarration> narrations)
        {
            var table = new DataTable("FactNarrations");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("Narration", typeof(string));
            table.Columns.Add("CreatedAt", typeof(DateTimeOffset));
            table.Columns.Add("UpdatedAt", typeof(DateTimeOffset));
            foreach (var n in narrations) table.Rows.Add(n.Id, n.OrganizationId, n.VoucherId, n.Narration, n.CreatedAt, n.UpdatedAt);
            return table;
        }

        private DataTable GetFactLedgerEntryDataTable(List<FactLedgerEntry> entries)
        {
            var table = new DataTable("FactLedgerEntries");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("LedgerId", typeof(Guid));
            table.Columns.Add("Debit", typeof(decimal));
            table.Columns.Add("Credit", typeof(decimal));
            table.Columns.Add("VoucherDate", typeof(DateTimeOffset));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("CreatedAt", typeof(DateTimeOffset));
            foreach (var e in entries) table.Rows.Add(e.Id, e.VoucherId, e.LedgerId, e.Debit, e.Credit, e.VoucherDate, e.OrganizationId, e.CreatedAt);
            return table;
        }

        private DataTable GetFactInventoryMovementDataTable(List<FactInventoryMovement> moves)
        {
            var table = new DataTable("FactInventoryMovements");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("StockItemId", typeof(Guid));
            table.Columns.Add("Quantity", typeof(decimal));
            table.Columns.Add("Amount", typeof(decimal));
            table.Columns.Add("VoucherDate", typeof(DateTimeOffset));
            table.Columns.Add("OrganizationId", typeof(Guid));
            foreach (var m in moves) table.Rows.Add(m.Id, m.VoucherId, m.StockItemId, m.Quantity, m.Amount, m.VoucherDate, m.OrganizationId);
            return table;
        }
        private DataTable GetBillAllocationsTable(List<BillAllocation> bills)
        {
            var table = new DataTable("BillAllocations");
            table.Columns.Add("Id", typeof(Guid));
            table.Columns.Add("OrganizationId", typeof(Guid));
            table.Columns.Add("VoucherId", typeof(Guid));
            table.Columns.Add("BillName", typeof(string));
            table.Columns.Add("BillType", typeof(string));
            table.Columns.Add("Amount", typeof(decimal));

            foreach (var b in bills)
            {
                table.Rows.Add(b.Id, b.OrganizationId, b.VoucherId, b.BillName, b.BillType, b.Amount);
            }
            return table;
        }
        #endregion
    }
}
