using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Acczite20.Models;

namespace Acczite20.Services.Authentication
{
    public class AuthenticationService : IAuthenticationService
    {
        private const string DefaultApiBaseUrl = "https://api.acczite.in";
        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;
        private readonly SessionPersistenceService _persistenceService;
        private readonly OrganizationContextService _organizationContextService;

        public AuthenticationService(
            HttpClient httpClient,
            IConfiguration configuration,
            SessionPersistenceService persistenceService,
            OrganizationContextService organizationContextService)
        {
            _httpClient = httpClient;
            _apiBaseUrl = (configuration["ApiBaseUrl"] ?? DefaultApiBaseUrl).TrimEnd('/');
            _persistenceService = persistenceService;
            _organizationContextService = organizationContextService;
        }

        public async Task<bool> LoginAsync(string identifier, string password)
        {
            try
            {
                var response = await _httpClient.PostAsJsonAsync($"{_apiBaseUrl}/api/user/signin", new
                {
                    identifier,
                    password
                });

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    throw new Exception($"HTTP {(int)response.StatusCode} {response.StatusCode}: {errorBody}");
                }

                var responseBody = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<LoginResponse>(responseBody, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                if (result == null || !result.Success)
                {
                    throw new Exception($"Server returned success but parsing failed: {responseBody}");
                }

                var session = SessionManager.Instance;
                var user = result.User;

                session.JwtToken = result.Token;
                session.IsAuthenticated = true;
                session.Username = user?.Username;
                session.Authority = user?.Authority;
                session.Permissions = user?.Permissions ?? new System.Collections.Generic.List<AuthPermission>();

                var userObjectId = user?.Id ?? string.Empty;
                if (Guid.TryParse(userObjectId, out var userId))
                {
                    session.UserId = userId;
                }
                session.UserObjectId = userObjectId;

                if (user?.Organization != null)
                {
                    if (Guid.TryParse(user.Organization.Id, out var orgId))
                    {
                        session.OrganizationId = orgId;
                    }

                    session.OrganizationObjectId = user.Organization.Id ?? string.Empty;
                    session.OrganizationName = user.Organization.Name;
                    session.IsTrialExpired = user.Organization.IsTrialExpired;
                }

                if (string.IsNullOrWhiteSpace(session.OrganizationObjectId))
                {
                    session.OrganizationObjectId = _organizationContextService.ExtractOrganizationIdFromJson(responseBody) ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(session.OrganizationName))
                {
                    session.OrganizationName = _organizationContextService.ExtractOrganizationNameFromJson(responseBody, session.OrganizationObjectId);
                }

                if (string.IsNullOrWhiteSpace(session.OrganizationObjectId) && !string.IsNullOrWhiteSpace(result.Token))
                {
                    session.OrganizationObjectId = _organizationContextService.ExtractOrganizationIdFromToken(result.Token) ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(session.OrganizationName) && !string.IsNullOrWhiteSpace(result.Token))
                {
                    session.OrganizationName = _organizationContextService.ExtractOrganizationNameFromToken(result.Token, session.OrganizationObjectId);
                }

                if (string.IsNullOrWhiteSpace(session.OrganizationName) && !string.IsNullOrWhiteSpace(session.OrganizationObjectId))
                {
                    session.OrganizationName = await _organizationContextService.ResolveOrganizationNameAsync(result.Token, session.OrganizationObjectId);
                }

                await _persistenceService.SaveSessionAsync(new UserSessionData
                {
                    Token = result.Token,
                    UserId = session.UserId != Guid.Empty ? session.UserId.ToString() : session.UserObjectId,
                    UserObjectId = session.UserObjectId,
                    Username = session.Username ?? string.Empty,
                    OrganizationId = session.OrganizationId != Guid.Empty ? session.OrganizationId.ToString() : session.OrganizationObjectId,
                    OrganizationObjectId = session.OrganizationObjectId,
                    OrganizationName = session.OrganizationName,
                    Authority = session.Authority ?? string.Empty,
                    IsTrialExpired = session.IsTrialExpired
                });

                return true;
            }
            catch (HttpRequestException ex)
            {
                throw new Exception($"Network connection failed: {ex.Message}");
            }
        }
    }
}
