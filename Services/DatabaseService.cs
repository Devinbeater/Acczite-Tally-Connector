using Acczite20.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Acczite20.Services
{
    public class DatabaseService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<DatabaseService> _logger;

        public DatabaseService(IServiceProvider serviceProvider, ILogger<DatabaseService> logger)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<(bool IsConnected, string Message)> TestDatabaseConnection()
        {
            try
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<AccziteDbContext>(); // Use AccziteDbContext
                    await dbContext.Database.CanConnectAsync();
                    _logger.LogInformation("Database connection successful.");
                    return (true, "Successfully connected to the database.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to connect to the database.");
                return (false, $"Failed to connect to the database: {ex.Message}");
            }
        }
    }
}