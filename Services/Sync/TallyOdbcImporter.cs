using System;
using System.Data.Odbc;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Acczite20.Data;
using Acczite20.Models;

using MongoDB.Bson;

namespace Acczite20.Services.Sync
{
    public class TallyOdbcImporter
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<TallyOdbcImporter> _logger;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly MongoService _mongoService;

        public TallyOdbcImporter(IServiceScopeFactory scopeFactory, ILogger<TallyOdbcImporter> logger, SyncStateMonitor syncMonitor, MongoService mongoService)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _syncMonitor = syncMonitor;
            _mongoService = mongoService;
        }

        private AppDbContext GetContext(IServiceProvider sp) => sp.GetRequiredService<AppDbContext>();

        private string GetOdbcConnectionString()
        {
            return $"Driver={{Tally ODBC Driver64}};Server=localhost;Port={SessionManager.Instance.TallyOdbcPort};";
        }

        public async Task ImportStockItemsAsync(Guid orgId, CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = GetContext(scope.ServiceProvider);
            _logger.LogInformation("Starting Stock Items ODBC import for Org: {OrgId}", orgId);
            var dbType = SessionManager.Instance.SelectedDatabaseType;
            bool isMongo = string.Equals(dbType, "MongoDB", StringComparison.OrdinalIgnoreCase);

            int synced = 0;
            try
            {
                using var conn = new OdbcConnection(GetOdbcConnectionString());
                await conn.OpenAsync(ct);

                var cmd = new OdbcCommand("SELECT $Name, $Parent, $ClosingBalance, $BaseUnits FROM StockItem", conn);
                using var reader = await cmd.ExecuteReaderAsync(ct);

                var mongoItems = new List<BsonDocument>();

                while (await reader.ReadAsync(ct))
                {
                    string name = reader["$Name"]?.ToString() ?? "";
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    string parent = reader["$Parent"]?.ToString() ?? "";
                    decimal closing = decimal.TryParse(reader["$ClosingBalance"]?.ToString(), out var cb) ? cb : 0;
                    string unit = reader["$BaseUnits"]?.ToString() ?? "";

                    var stockItem = await dbContext.StockItems
                        .IgnoreQueryFilters()
                        .FirstOrDefaultAsync(s => s.OrganizationId == orgId && s.Name == name, ct);

                    if (stockItem == null)
                    {
                        stockItem = new StockItem
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            Name = name,
                            StockGroup = parent,
                            ClosingBalance = closing,
                            BaseUnit = unit,
                            TallyMasterId = name
                        };
                        dbContext.StockItems.Add(stockItem);
                    }
                    else
                    {
                        stockItem.StockGroup = parent;
                        stockItem.ClosingBalance = closing;
                        stockItem.BaseUnit = unit;
                    }

                    if (isMongo)
                    {
                        mongoItems.Add(new BsonDocument {
                            { "name", name },
                            { "stockGroup", parent },
                            { "closingBalance", (double)closing },
                            { "unit", unit },
                            { "TallyMasterId", name }
                        });
                        
                        if (mongoItems.Count >= 100)
                        {
                            await _mongoService.BulkUpsertDocumentsAsync("stockitems", mongoItems, "TallyMasterId");
                            mongoItems.Clear();
                        }
                    }

                    synced++;
                    if (synced % 100 == 0) await dbContext.SaveChangesAsync(ct);
                }

                if (isMongo && mongoItems.Any())
                {
                    await _mongoService.BulkUpsertDocumentsAsync("stockitems", mongoItems, "TallyMasterId");
                }

                await dbContext.SaveChangesAsync(ct);
                _syncMonitor.AddLog($"Successfully pulled {synced} Stock Items via ODBC.", "SUCCESS");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to import Stock Items via ODBC");
                _syncMonitor.AddLog($"ODBC StockItem Import failed: {ex.Message}", "ERROR");
            }
        }

        public async Task ImportLedgersAsync(Guid orgId, CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = GetContext(scope.ServiceProvider);
            _logger.LogInformation("Starting Ledgers ODBC import for Org: {OrgId}", orgId);
            var dbType = SessionManager.Instance.SelectedDatabaseType;
            bool isMongo = string.Equals(dbType, "MongoDB", StringComparison.OrdinalIgnoreCase);

            int synced = 0;
            try
            {
                using var conn = new OdbcConnection(GetOdbcConnectionString());
                await conn.OpenAsync(ct);

                var cmd = new OdbcCommand("SELECT $Name, $Parent, $OpeningBalance, $ClosingBalance FROM Ledger", conn);
                using var reader = await cmd.ExecuteReaderAsync(ct);

                var mongoLedgers = new List<BsonDocument>();

                while (await reader.ReadAsync(ct))
                {
                    string name = reader["$Name"]?.ToString() ?? "";
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    string parent = reader["$Parent"]?.ToString() ?? "";
                    decimal opening = decimal.TryParse(reader["$OpeningBalance"]?.ToString(), out var ob) ? ob : 0;
                    decimal closing = decimal.TryParse(reader["$ClosingBalance"]?.ToString(), out var cb) ? cb : 0;

                    var ledger = await dbContext.Ledgers
                        .IgnoreQueryFilters()
                        .FirstOrDefaultAsync(l => l.OrganizationId == orgId && l.Name == name, ct);

                    if (ledger == null)
                    {
                        ledger = new Ledger
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            Name = name,
                            ParentGroup = parent,
                            OpeningBalance = opening,
                            ClosingBalance = closing,
                            TallyMasterId = name
                        };
                        dbContext.Ledgers.Add(ledger);
                    }
                    else
                    {
                        ledger.ParentGroup = parent;
                        ledger.OpeningBalance = opening;
                        ledger.ClosingBalance = closing;
                    }

                    if (isMongo)
                    {
                        mongoLedgers.Add(new BsonDocument {
                            { "name", name },
                            { "parentGroup", parent },
                            { "openingBalance", (double)opening },
                            { "closingBalance", (double)closing },
                            { "TallyMasterId", name }
                        });

                        if (mongoLedgers.Count >= 100)
                        {
                            await _mongoService.BulkUpsertDocumentsAsync("ledgers", mongoLedgers, "TallyMasterId");
                            mongoLedgers.Clear();
                        }
                    }

                    synced++;
                    if (synced % 100 == 0) await dbContext.SaveChangesAsync(ct);
                }

                if (isMongo && mongoLedgers.Any())
                {
                    await _mongoService.BulkUpsertDocumentsAsync("ledgers", mongoLedgers, "TallyMasterId");
                }

                await dbContext.SaveChangesAsync(ct);
                _syncMonitor.AddLog($"Successfully pulled {synced} Ledgers via ODBC.", "SUCCESS");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to import Ledgers via ODBC");
                _syncMonitor.AddLog($"ODBC Ledger Import failed: {ex.Message}", "ERROR");
            }
        }
    }
}
