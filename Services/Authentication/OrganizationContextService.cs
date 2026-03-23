using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Acczite20.Services.Authentication
{
    public class OrganizationContextService
    {
        private const string DefaultApiBaseUrl = "https://api.acczite.in";

        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;
        private readonly MongoService _mongoService;

        public OrganizationContextService(HttpClient httpClient, IConfiguration configuration, MongoService mongoService)
        {
            _httpClient = httpClient;
            _apiBaseUrl = (configuration["ApiBaseUrl"] ?? DefaultApiBaseUrl).TrimEnd('/');
            _mongoService = mongoService;
        }

        public string? ExtractOrganizationIdFromJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                return null;
            }

            try
            {
                using var document = JsonDocument.Parse(json);
                return ExtractOrganizationId(document.RootElement);
            }
            catch
            {
                return null;
            }
        }

        public string? ExtractOrganizationNameFromJson(string json, string? organizationId = null)
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                return null;
            }

            try
            {
                using var document = JsonDocument.Parse(json);
                return ExtractOrganizationName(document.RootElement, organizationId);
            }
            catch
            {
                return null;
            }
        }

        public string? ExtractOrganizationId(JsonElement element)
        {
            return FindOrganizationId(element);
        }

        public string? ExtractOrganizationName(JsonElement element, string? organizationId = null)
        {
            return FindOrganizationName(element, organizationId, contextName: null);
        }

        public string? ExtractOrganizationIdFromToken(string? token)
        {
            if (!TryParseJwtPayload(token, out var payload))
            {
                return null;
            }

            try
            {
                return ExtractOrganizationId(payload.RootElement);
            }
            finally
            {
                payload.Dispose();
            }
        }

        public string? ExtractOrganizationNameFromToken(string? token, string? organizationId = null)
        {
            if (!TryParseJwtPayload(token, out var payload))
            {
                return null;
            }

            try
            {
                return ExtractOrganizationName(payload.RootElement, organizationId);
            }
            finally
            {
                payload.Dispose();
            }
        }

        public async Task<string?> ResolveOrganizationNameAsync(string? token, string? organizationId, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(organizationId))
            {
                return null;
            }

            var tokenName = ExtractOrganizationNameFromToken(token, organizationId);
            if (!string.IsNullOrWhiteSpace(tokenName))
            {
                return tokenName;
            }

            var mongoName = await TryResolveFromMongoAsync(organizationId, cancellationToken);
            if (!string.IsNullOrWhiteSpace(mongoName))
            {
                return mongoName;
            }

            return await TryResolveFromApiAsync(token, organizationId, cancellationToken);
        }

        private async Task<string?> TryResolveFromMongoAsync(string organizationId, CancellationToken cancellationToken)
        {
            try
            {
                var collection = await _mongoService.GetCollectionAsync("organizations", "organization", "organisations", "orgs");
                if (collection == null)
                {
                    return null;
                }

                var builder = Builders<BsonDocument>.Filter;
                var filters = new List<FilterDefinition<BsonDocument>>
                {
                    builder.Eq("_id", organizationId),
                    builder.Eq("id", organizationId),
                    builder.Eq("organizationId", organizationId)
                };

                if (ObjectId.TryParse(organizationId, out var objectId))
                {
                    filters.Add(builder.Eq("_id", objectId));
                    filters.Add(builder.Eq("id", objectId));
                    filters.Add(builder.Eq("organizationId", objectId));
                }

                var document = await collection.Find(builder.Or(filters)).FirstOrDefaultAsync(cancellationToken);
                return document == null ? null : ExtractOrganizationNameFromJson(document.ToJson(), organizationId);
            }
            catch
            {
                return null;
            }
        }

        private async Task<string?> TryResolveFromApiAsync(string? token, string organizationId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                return null;
            }

            foreach (var endpoint in GetCandidateEndpoints(organizationId))
            {
                var name = await TryGetOrganizationNameAsync(endpoint, token, organizationId, cancellationToken);
                if (!string.IsNullOrWhiteSpace(name))
                {
                    return name;
                }
            }

            return null;
        }

        private IEnumerable<string> GetCandidateEndpoints(string organizationId)
        {
            var encodedId = Uri.EscapeDataString(organizationId);
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var endpoint in new[]
            {
                $"{_apiBaseUrl}/api/user/profile",
                $"{_apiBaseUrl}/api/user/me",
                $"{_apiBaseUrl}/api/user/current",
                $"{_apiBaseUrl}/api/organization/{encodedId}",
                $"{_apiBaseUrl}/api/organizations/{encodedId}",
                $"{_apiBaseUrl}/api/organization/current"
            })
            {
                if (seen.Add(endpoint))
                {
                    yield return endpoint;
                }
            }
        }

        private async Task<string?> TryGetOrganizationNameAsync(string endpoint, string token, string organizationId, CancellationToken cancellationToken)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(4));

                using var response = await _httpClient.SendAsync(request, timeoutCts.Token);
                if (!response.IsSuccessStatusCode)
                {
                    return null;
                }

                var json = await response.Content.ReadAsStringAsync(timeoutCts.Token);
                return ExtractOrganizationNameFromJson(json, organizationId);
            }
            catch
            {
                return null;
            }
        }

        private static bool TryParseJwtPayload(string? token, out JsonDocument payload)
        {
            payload = null!;

            if (string.IsNullOrWhiteSpace(token))
            {
                return false;
            }

            try
            {
                var parts = token.Split('.');
                if (parts.Length < 2)
                {
                    return false;
                }

                var base64 = parts[1].Replace('-', '+').Replace('_', '/');
                switch (base64.Length % 4)
                {
                    case 2:
                        base64 += "==";
                        break;
                    case 3:
                        base64 += "=";
                        break;
                }

                var json = Encoding.UTF8.GetString(Convert.FromBase64String(base64));
                payload = JsonDocument.Parse(json);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static string? FindOrganizationId(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var propertyName in new[] { "orgId", "organizationId", "organizationAssigned", "organizationObjectId", "organization" })
                {
                    if (TryGetPropertyIgnoreCase(element, propertyName, out var propertyValue))
                    {
                        if (propertyValue.ValueKind == JsonValueKind.String)
                        {
                            return Normalize(propertyValue.GetString());
                        }

                        if (propertyValue.ValueKind == JsonValueKind.Object && TryReadIdLike(propertyValue, out var nestedId))
                        {
                            return nestedId;
                        }
                    }
                }

                foreach (var property in element.EnumerateObject())
                {
                    var nestedId = FindOrganizationId(property.Value);
                    if (!string.IsNullOrWhiteSpace(nestedId))
                    {
                        return nestedId;
                    }
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    var nestedId = FindOrganizationId(item);
                    if (!string.IsNullOrWhiteSpace(nestedId))
                    {
                        return nestedId;
                    }
                }
            }

            return null;
        }

        private static string? FindOrganizationName(JsonElement element, string? organizationId, string? contextName)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var directNameProperty in new[] { "organizationName", "orgName", "organisationName", "organizationDisplayName" })
                {
                    if (TryGetPropertyIgnoreCase(element, directNameProperty, out var propertyValue) &&
                        propertyValue.ValueKind == JsonValueKind.String)
                    {
                        return Normalize(propertyValue.GetString());
                    }
                }

                var isOrganizationObject =
                    IsOrganizationContext(contextName) ||
                    (TryReadIdLike(element, out var elementId) &&
                     !string.IsNullOrWhiteSpace(organizationId) &&
                     string.Equals(elementId, organizationId, StringComparison.OrdinalIgnoreCase));

                if (isOrganizationObject)
                {
                    foreach (var objectNameProperty in new[] { "name", "displayName", "title" })
                    {
                        if (TryGetPropertyIgnoreCase(element, objectNameProperty, out var propertyValue) &&
                            propertyValue.ValueKind == JsonValueKind.String)
                        {
                            return Normalize(propertyValue.GetString());
                        }
                    }
                }

                foreach (var property in element.EnumerateObject())
                {
                    var nestedName = FindOrganizationName(property.Value, organizationId, property.Name);
                    if (!string.IsNullOrWhiteSpace(nestedName))
                    {
                        return nestedName;
                    }
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    var nestedName = FindOrganizationName(item, organizationId, contextName);
                    if (!string.IsNullOrWhiteSpace(nestedName))
                    {
                        return nestedName;
                    }
                }
            }

            return null;
        }

        private static bool TryGetPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement value)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                    {
                        value = property.Value;
                        return true;
                    }
                }
            }

            value = default;
            return false;
        }

        private static bool TryReadIdLike(JsonElement element, out string? id)
        {
            foreach (var propertyName in new[] { "id", "_id", "organizationId", "orgId" })
            {
                if (TryGetPropertyIgnoreCase(element, propertyName, out var propertyValue) &&
                    propertyValue.ValueKind == JsonValueKind.String)
                {
                    id = Normalize(propertyValue.GetString());
                    return !string.IsNullOrWhiteSpace(id);
                }
            }

            id = null;
            return false;
        }

        private static bool IsOrganizationContext(string? contextName)
        {
            return !string.IsNullOrWhiteSpace(contextName) &&
                   (contextName.Contains("organization", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(contextName, "org", StringComparison.OrdinalIgnoreCase));
        }

        private static string? Normalize(string? value)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
        }
    }
}
