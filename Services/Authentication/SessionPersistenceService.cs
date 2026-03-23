using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Security.Cryptography;

namespace Acczite20.Services.Authentication
{
    public class UserSessionData
    {
        public string Token { get; set; } = string.Empty;
        public string UserId { get; set; } = string.Empty;
        public string UserObjectId { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string OrganizationId { get; set; } = string.Empty;
        public string OrganizationObjectId { get; set; } = string.Empty;
        public string? OrganizationName { get; set; }
        public string Authority { get; set; } = string.Empty;
        public bool IsTrialExpired { get; set; }
    }

    public class SessionPersistenceService
    {
        private readonly string _filePath;

        public SessionPersistenceService()
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            var folder = Path.Combine(appData, "Acczite");
            if (!Directory.Exists(folder)) Directory.CreateDirectory(folder);
            _filePath = Path.Combine(folder, "session.dat");
        }

        public async Task SaveSessionAsync(UserSessionData data)
        {
            try
            {
                var json = JsonSerializer.Serialize(data);
                var rawBytes = Encoding.UTF8.GetBytes(json);
                
                // Secure storage using Windows DPAPI
                var encryptedBytes = ProtectedData.Protect(rawBytes, null, DataProtectionScope.CurrentUser);
                
                await File.WriteAllBytesAsync(_filePath, encryptedBytes).ConfigureAwait(false);
            }
            catch { }
        }

        public async Task<UserSessionData?> LoadSessionAsync()
        {
            try
            {
                if (!File.Exists(_filePath)) return null;

                var encryptedBytes = await File.ReadAllBytesAsync(_filePath).ConfigureAwait(false);
                
                // Decrypt using Windows DPAPI
                var rawBytes = ProtectedData.Unprotect(encryptedBytes, null, DataProtectionScope.CurrentUser);
                
                var json = Encoding.UTF8.GetString(rawBytes);
                return JsonSerializer.Deserialize<UserSessionData>(json);
            }
            catch
            {
                return null;
            }
        }

        public void ClearSession()
        {
            try
            {
                if (File.Exists(_filePath)) File.Delete(_filePath);
            }
            catch { }
        }
    }
}
