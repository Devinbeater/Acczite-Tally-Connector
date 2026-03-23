using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Acczite20.Models.Integration;
using Acczite20.Services.Authentication;

namespace Acczite20.Services.Integration
{
    public interface IMernIntegrationService
    {
        Task<List<MernProduct>> SyncProductsAsync(Guid orgId);
        Task<List<MernEmployee>> SyncEmployeesAsync(Guid orgId);
        Task<List<MernAttendance>> SyncAttendanceAsync(Guid orgId, DateTime fromDate);
    }

    public class MernIntegrationService : IMernIntegrationService
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly IEntityMappingService _mappingService;

        public MernIntegrationService(HttpClient httpClient, IConfiguration configuration, IEntityMappingService mappingService)
        {
            _httpClient = httpClient;
            _baseUrl = (configuration["ApiBaseUrl"] ?? "https://api.acczite.in").TrimEnd('/');
            _mappingService = mappingService;
        }

        private void SetAuthHeader()
        {
            var token = SessionManager.Instance.JwtToken;
            if (!string.IsNullOrEmpty(token))
            {
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }
        }

        public async Task<List<MernProduct>> SyncProductsAsync(Guid orgId)
        {
            try
            {
                SetAuthHeader();
                // Assumed MERN endpoint
                var products = await _httpClient.GetFromJsonAsync<List<MernProduct>>($"{_baseUrl}/api/products");
                return products ?? new List<MernProduct>();
            }
            catch { return new List<MernProduct>(); }
        }

        public async Task<List<MernEmployee>> SyncEmployeesAsync(Guid orgId)
        {
            try
            {
                SetAuthHeader();
                // Assumed MERN endpoint
                var employees = await _httpClient.GetFromJsonAsync<List<MernEmployee>>($"{_baseUrl}/api/hr/employees");
                return employees ?? new List<MernEmployee>();
            }
            catch { return new List<MernEmployee>(); }
        }

        public async Task<List<MernAttendance>> SyncAttendanceAsync(Guid orgId, DateTime fromDate)
        {
            try
            {
                SetAuthHeader();
                // Assumed MERN endpoint
                var attendance = await _httpClient.GetFromJsonAsync<List<MernAttendance>>($"{_baseUrl}/api/hr/attendance?from={fromDate:yyyy-MM-dd}");
                return attendance ?? new List<MernAttendance>();
            }
            catch { return new List<MernAttendance>(); }
        }
    }
}
