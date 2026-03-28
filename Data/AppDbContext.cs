using Acczite20.Models;
using Acczite20.Services;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Acczite20.Models.Warehouse;

namespace Acczite20.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        // ── Entity Sets ──
        public DbSet<Company> Companies { get; set; }
        public DbSet<VoucherType> VoucherTypes { get; set; }
        public DbSet<Voucher> Vouchers { get; set; }
        public DbSet<InventoryAllocation> InventoryAllocations { get; set; }
        public DbSet<GstBreakdown> GstBreakdowns { get; set; }
        public DbSet<LedgerEntry> LedgerEntries { get; set; }
        public DbSet<Ledger> Ledgers { get; set; }
        public DbSet<StockItem> StockItems { get; set; }
        public DbSet<Invoice> Invoices { get; set; }
        public DbSet<Expense> Expenses { get; set; }
        public DbSet<ContraEntry> ContraEntries { get; set; }
        public DbSet<Payroll> Payrolls { get; set; }
        public DbSet<Payslip> Payslips { get; set; }
        public DbSet<SyncMetadata> SyncMetadataRecords { get; set; }
        public DbSet<SyncLog> SyncLogs { get; set; }
        public DbSet<DeadLetter> DeadLetters { get; set; }
        public DbSet<MongoProjectionQueue> MongoProjectionQueue { get; set; }
        public DbSet<TallySyncConfiguration> TallySyncConfigurations { get; set; }
        
        // Integration Layer (MERN <-> Tally <-> WPF)
        public DbSet<Acczite20.Models.Integration.MernMapping> MernMappings { get; set; }
        public DbSet<Acczite20.Models.Integration.MernProduct> MernProducts { get; set; }
        public DbSet<Acczite20.Models.Integration.MernEmployee> MernEmployees { get; set; }
        public DbSet<Acczite20.Models.Integration.MernAttendance> MernAttendances { get; set; }
        public DbSet<Acczite20.Models.Integration.IntegrationAuditLog> IntegrationAuditLogs { get; set; }
        public DbSet<Acczite20.Models.Integration.PendingMapping> PendingMappings { get; set; }
        public DbSet<Acczite20.Models.Integration.IntegrationEventQueue> IntegrationEventQueues { get; set; }
        
        // Master Data
        public DbSet<AccountingGroup> AccountingGroups { get; set; }
        public DbSet<Currency> Currencies { get; set; }
        public DbSet<StockGroup> StockGroups { get; set; }
        public DbSet<StockCategory> StockCategories { get; set; }
        public DbSet<Unit> Units { get; set; }
        public DbSet<Godown> Godowns { get; set; }
        public DbSet<CostCategory> CostCategories { get; set; }
        public DbSet<CostCentre> CostCentres { get; set; }

        // ── Warehouse Fact & Dimension Tables ──
        public DbSet<FactVoucher> FactVouchers { get; set; }
        public DbSet<FactLedgerEntry> FactLedgerEntries { get; set; }
        public DbSet<FactInventoryMovement> FactInventoryMovements { get; set; }
        public DbSet<FactTaxEntry> FactTaxEntries { get; set; }
        public DbSet<FactNarration> FactNarrations { get; set; }
        public DbSet<DimLedger> DimLedgers { get; set; }
        public DbSet<DimGroup> DimGroups { get; set; }
        public DbSet<DimStockItem> DimStockItems { get; set; }
        public DbSet<DimVoucherType> DimVoucherTypes { get; set; }
        public DbSet<LedgerBalanceSnapshot> LedgerBalanceSnapshots { get; set; }
        public DbSet<Acczite20.Models.Warehouse.LedgerMapping> LedgerMappings { get; set; }
        public DbSet<Acczite20.Models.Analytics.BusinessPulseSnapshot> BusinessPulseSnapshots { get; set; }
        
        // History & Auditing
        public DbSet<Acczite20.Models.History.UnifiedActivityLog> UnifiedActivityLogs { get; set; }

        // ════════════════════════════════════════════════════
        //  BULLETPROOF ORG ISOLATION + SOFT DELETE FILTER
        // ════════════════════════════════════════════════════
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Apply global query filters to ALL BaseEntity types automatically
            foreach (var entityType in modelBuilder.Model.GetEntityTypes())
            {
                if (typeof(BaseEntity).IsAssignableFrom(entityType.ClrType))
                {
                    var method = typeof(AppDbContext)
                        .GetMethod(nameof(SetGlobalQueryFilter),
                            BindingFlags.NonPublic | BindingFlags.Static)
                        ?.MakeGenericMethod(entityType.ClrType);

                    method?.Invoke(null, new object[] { modelBuilder });
                }
            }

            // ── Indexes ──
            modelBuilder.Entity<Voucher>()
                .HasIndex(v => v.VoucherNumber);
            modelBuilder.Entity<Voucher>()
                .HasIndex(v => v.LastModified);
            modelBuilder.Entity<Voucher>()
                .HasIndex(v => new { v.CompanyId, v.VoucherDate });
            modelBuilder.Entity<Voucher>()
                .HasIndex(v => new { v.CompanyId, v.TallyMasterId })
                .IsUnique()
                .HasFilter("[TallyMasterId] IS NOT NULL");

            // UNIQUE BUSINESS KEY (Enterprise Integrity)
            modelBuilder.Entity<Voucher>()
                .HasIndex(v => new { v.OrganizationId, v.VoucherNumber, v.VoucherDate })
                .IsUnique()
                .HasFilter("[VoucherNumber] IS NOT NULL");

            // SEARCH INDEXES (Performance)
            modelBuilder.Entity<Ledger>()
                .HasIndex(l => new { l.OrganizationId, l.Name });

            modelBuilder.Entity<AccountingGroup>()
                .HasIndex(a => new { a.OrganizationId, a.Name });

            modelBuilder.Entity<MongoProjectionQueue>()
                .HasIndex(m => new { m.OrganizationId, m.CreatedAt });

            // ── Warehouse Indexes (High Performance) ──
            modelBuilder.Entity<FactVoucher>()
                .HasIndex(f => new { f.OrganizationId, f.VoucherDate });
            modelBuilder.Entity<FactVoucher>()
                .HasIndex(f => f.VoucherTypeId);

            modelBuilder.Entity<FactLedgerEntry>()
                .HasIndex(f => new { f.OrganizationId, f.LedgerId, f.VoucherDate });
            modelBuilder.Entity<FactLedgerEntry>()
                .HasIndex(f => f.VoucherId);

            modelBuilder.Entity<FactInventoryMovement>()
                .HasIndex(f => new { f.OrganizationId, f.StockItemId, f.VoucherDate });

            modelBuilder.Entity<LedgerBalanceSnapshot>()
                .HasIndex(f => new { f.OrganizationId, f.LedgerId, f.Year, f.Month });

            // Org index on every entity for fast filtering
            modelBuilder.Entity<Company>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<Voucher>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<SyncMetadata>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<DeadLetter>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<TallySyncConfiguration>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<FactVoucher>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<FactLedgerEntry>().HasIndex(e => e.OrganizationId);
            modelBuilder.Entity<DimLedger>().HasIndex(e => e.OrganizationId);

            // ── Relationships ──
            modelBuilder.Entity<Voucher>()
                .HasOne(v => v.Company)
                .WithMany()
                .HasForeignKey(v => v.CompanyId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<Voucher>()
                .HasOne(v => v.VoucherType)
                .WithMany()
                .HasForeignKey(v => v.VoucherTypeId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<LedgerEntry>()
                .HasOne(l => l.Voucher)
                .WithMany(v => v.LedgerEntries)
                .HasForeignKey(l => l.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<InventoryAllocation>()
                .HasOne(i => i.Voucher)
                .WithMany(v => v.InventoryAllocations)
                .HasForeignKey(i => i.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<GstBreakdown>()
                .HasOne(g => g.Voucher)
                .WithMany(v => v.GstBreakdowns)
                .HasForeignKey(g => g.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<Invoice>()
                .HasOne(i => i.Voucher)
                .WithMany()
                .HasForeignKey(i => i.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<Expense>()
                .HasOne(e => e.Voucher)
                .WithMany()
                .HasForeignKey(e => e.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<ContraEntry>()
                .HasOne(c => c.Voucher)
                .WithMany()
                .HasForeignKey(c => c.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<Payroll>()
                .HasOne(p => p.Voucher)
                .WithMany()
                .HasForeignKey(p => p.VoucherId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<Payslip>()
                .HasOne(p => p.Payroll)
                .WithMany()
                .HasForeignKey(p => p.PayrollId)
                .OnDelete(DeleteBehavior.Restrict);

            modelBuilder.Entity<SyncMetadata>()
                .HasOne(s => s.Company)
                .WithMany()
                .HasForeignKey(s => s.CompanyId)
                .OnDelete(DeleteBehavior.Cascade);

            // Timeline Indexes
            modelBuilder.Entity<Acczite20.Models.History.UnifiedActivityLog>()
                .HasIndex(u => new { u.OrganizationId, u.Timestamp });

            modelBuilder.Entity<Acczite20.Models.History.UnifiedActivityLog>()
                .HasIndex(u => new { u.EntityType, u.EntityId });
        }

        /// <summary>
        /// Applies a global query filter combining soft-delete AND organization isolation.
        /// SessionManager.OrganizationId is read at query execution time (not model build time).
        /// </summary>
        private static void SetGlobalQueryFilter<TEntity>(ModelBuilder builder)
            where TEntity : BaseEntity
        {
            builder.Entity<TEntity>()
                .HasQueryFilter(e =>
                    !e.IsDeleted &&
                    e.OrganizationId == SessionManager.Instance.OrganizationId);
        }

        // ════════════════════════════════════════════════════
        //  AUDIT FIELDS AUTO-FILL + ORG STAMP
        // ════════════════════════════════════════════════════
        public override int SaveChanges()
        {
            StampAuditFields();
            return base.SaveChanges();
        }

        public override Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
        {
            StampAuditFields();
            return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        }

        private void StampAuditFields()
        {
            var entries = ChangeTracker.Entries<BaseEntity>()
                .Where(e => e.State == EntityState.Added || e.State == EntityState.Modified);

            foreach (var entry in entries)
            {
                if (entry.State == EntityState.Added)
                {
                    entry.Entity.CreatedAt = DateTimeOffset.UtcNow;
                    // Stamp org on insert — NEVER allow cross-org insertion
                    if (entry.Entity.OrganizationId == Guid.Empty)
                    {
                        entry.Entity.OrganizationId = SessionManager.Instance.OrganizationId;
                    }
                }
                entry.Entity.UpdatedAt = DateTimeOffset.UtcNow;
            }
        }
    }
}
