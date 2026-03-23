using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Acczite20.Models
{
    public class LoginResponse
    {
        [JsonPropertyName("success")]
        public bool Success { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }

        [JsonPropertyName("token")]
        public string Token { get; set; }

        [JsonPropertyName("user")]
        public AuthUser User { get; set; }
    }

    public class AuthUser
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("username")]
        public string Username { get; set; }

        [JsonPropertyName("email")]
        public string Email { get; set; }

        [JsonPropertyName("authority")]
        public string Authority { get; set; }

        [JsonPropertyName("permissions")]
        public List<AuthPermission> Permissions { get; set; } = new List<AuthPermission>();

        [JsonPropertyName("organization")]
        public AuthOrganization Organization { get; set; }
    }

    public class AuthPermission
    {
        [JsonPropertyName("sectionKey")]
        public string SectionKey { get; set; }

        [JsonPropertyName("actions")]
        public List<string> Actions { get; set; } = new List<string>();
    }

    public class AuthOrganization
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("isTrialExpired")]
        public bool IsTrialExpired { get; set; }
    }
}
