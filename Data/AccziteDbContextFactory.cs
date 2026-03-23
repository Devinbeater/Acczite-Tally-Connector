using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace Acczite20.Data
{
    public class AccziteDbContextFactory : IAccziteDbContextFactory
    {
        private readonly IConfiguration _configuration;

        public AccziteDbContextFactory(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public AccziteDbContext CreateDbContext()
        {
            var optionsBuilder = new DbContextOptionsBuilder<AccziteDbContext>();
            string? connectionString = _configuration.GetConnectionString("ServerDatabase");
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new InvalidOperationException("Database connection string 'ServerDatabase' is missing or empty in configuration.");

            optionsBuilder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString));
            return new AccziteDbContext(optionsBuilder.Options, _configuration);
        }
    }
}
