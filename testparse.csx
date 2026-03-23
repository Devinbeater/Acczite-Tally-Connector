using System;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;

public class LoginResponse {
    [JsonPropertyName("success")] public bool Success { get; set; }
    [JsonPropertyName("message")] public string Message { get; set; }
    [JsonPropertyName("token")] public string Token { get; set; }
    [JsonPropertyName("user")] public AuthUser User { get; set; }
}
public class AuthUser {
    [JsonPropertyName("id")] public string Id { get; set; }
    [JsonPropertyName("username")] public string Username { get; set; }
    [JsonPropertyName("authority")] public string Authority { get; set; }
    [JsonPropertyName("email")] public string Email { get; set; }
    [JsonPropertyName("organization")] public AuthOrganization Organization { get; set; }
    [JsonPropertyName("permissions")] public List<AuthPermission> Permissions { get; set; } = new List<AuthPermission>();
}
public class AuthPermission {
    [JsonPropertyName("sectionKey")] public string SectionKey { get; set; }
    [JsonPropertyName("actions")] public List<string> Actions { get; set; } = new List<string>();
}
public class AuthOrganization {
    [JsonPropertyName("id")] public string Id { get; set; }
    [JsonPropertyName("name")] public string Name { get; set; }
    [JsonPropertyName("isTrialExpired")] public bool IsTrialExpired { get; set; }
}

var json = @"{
    \""success\"":  true,
    \""message\"":  \""Login successful.\"",
    \""token\"":  \""test\"",
    \""user\"":  {
                 \""id\"":  \""69522de1eea3d030b45373b4\"",
                 \""username\"":  \""adminTest\"",
                 \""authority\"":  \""admin\"",
                 \""email\"":  \""admintest@gmail.com\"",
                 \""isVerified\"":  true,
                 \""supplierRole\"":  null,
                 \""workerType\"":  null,
                 \""amcstaffroles\"":  null,
                 \""amcadminroles\"":  \""FullAccessAdmin\"",
                 \""organization\"":  null
             }
}";
try {
    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    var result = JsonSerializer.Deserialize<LoginResponse>(json, options);
    Console.WriteLine("Success: " + result.Success);
} catch (Exception ex) {
    Console.WriteLine("JSON Error: " + ex.ToString());
}
