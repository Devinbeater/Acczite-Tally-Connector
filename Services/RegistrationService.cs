using System;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using Microsoft.Data.SqlClient; // ✅ Use only this for SqlException

namespace Acczite20.Services
{
    public class RegistrationService
    {
        private readonly IAccziteDbContextFactory _dbContextFactory;

        public RegistrationService(IAccziteDbContextFactory dbContextFactory)
        {
            _dbContextFactory = dbContextFactory ?? throw new ArgumentNullException(nameof(dbContextFactory));
        }

        public async Task<(bool IsSuccess, string Message)> RegisterUserAsync(User user, string password)
        {
            try
            {
                // Placeholder hash logic
                (string passwordHash, string salt) = HashPassword(password);
                user.PasswordHash = passwordHash;
                user.Salt = salt;
                user.RegistrationDate = DateTime.UtcNow;

                using var dbContext = _dbContextFactory.CreateDbContext();

                // Check for existing username
                if (await dbContext.Users.AnyAsync(u => u.Username == user.Username))
                {
                    return (false, "Username already exists.");
                }

                dbContext.Users.Add(user);
                await dbContext.SaveChangesAsync();
                return (true, "Registration successful!");
            }
            catch (Exception ex) when (
                ex is DbUpdateException ||
                ex is SqlException ||
                ex is MySqlException)
            {
                Console.Error.WriteLine($"Database connection error during registration: {ex.Message}");
                return (false, "Could not register the user. Please check your internet connection and try again.");
            }
        }

        private (string Hash, string Salt) HashPassword(string password)
        {
            // TODO: Replace with real password hashing logic
            return ("hashedPassword", "salt");
        }
    }
}
