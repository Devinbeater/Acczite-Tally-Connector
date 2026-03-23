// Acczite20/Data/AccziteDbContext.cs
using Acczite20.Models; // ✅ Corrected namespace
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System;

namespace Acczite20.Data // ✅ Make sure this namespace matches your project folder structure
{
    public class AccziteDbContext : DbContext
    {
        private readonly IConfiguration _configuration;
        public string ConnectionStatusMessage { get; private set; } = "Connecting to the database...";

        public AccziteDbContext(DbContextOptions<AccziteDbContext> options, IConfiguration configuration)
            : base(options)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public DbSet<User> Users { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                try
                {
                    string? connectionString = _configuration.GetConnectionString("ServerDatabase");
                    optionsBuilder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString));
                    ConnectionStatusMessage = "Connected to the database.";
                    Console.WriteLine("✅ Connected to Server Database");
                }
                catch (MySqlException ex)
                {
                    ConnectionStatusMessage = "❌ Could not connect to the database. Please check your internet connection or server status.";
                    Console.WriteLine($"MySQL Connection Error: {ex.Message}");
                }
                catch (Exception ex)
                {
                    ConnectionStatusMessage = "⚠️ An unexpected error occurred while connecting to the database.";
                    Console.WriteLine($"Unexpected Connection Error: {ex.Message}");
                }
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<User>(entity =>
            {
                entity.ToTable("users");
                entity.HasKey(u => u.Id);
                entity.Property(u => u.FullName).HasColumnName("full_name");
                entity.Property(u => u.Email).HasColumnName("email");
                entity.Property(u => u.Mobile).HasColumnName("mobile");
                entity.Property(u => u.Username).HasColumnName("username");
                entity.Property(u => u.PasswordHash).HasColumnName("password_hash");
                entity.Property(u => u.Salt).HasColumnName("salt");
                entity.Property(u => u.IsActive).HasColumnName("is_active");
                entity.Property(u => u.IsTallyUser).HasColumnName("is_tally_user");
                entity.Property(u => u.RegistrationDate).HasColumnName("registration_date");
                entity.Property(u => u.RenewalType).HasColumnName("renewal_type");
            });
        }
    }
}
