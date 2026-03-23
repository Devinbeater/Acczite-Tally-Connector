using System;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Infrastructure
{
    public static class DatabaseProviderFactory
    {
        public static void Configure(DbContextOptionsBuilder builder, string dbType, string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) return;

            if (dbType.Equals("SQL Server", StringComparison.OrdinalIgnoreCase))
            {
                builder.UseSqlServer(connectionString, options =>
                {
                    options.EnableRetryOnFailure(5, TimeSpan.FromSeconds(10), null);
                });
            }
            else if (dbType.Equals("MySQL", StringComparison.OrdinalIgnoreCase))
            {
                builder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString), options =>
                {
                    options.EnableRetryOnFailure(5, TimeSpan.FromSeconds(10), null);
                });
            }
            else if (dbType.Equals("MongoDB", StringComparison.OrdinalIgnoreCase))
            {
                // MongoDB is handled via MongoService, EF used as fallback cache
                builder.UseInMemoryDatabase("AccziteMongoCache");
            }
            else
            {
                throw new NotSupportedException($"Database provider '{dbType}' is not supported.");
            }
        }
    }
}
