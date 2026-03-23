using System;
using System.Collections.Generic;
using Acczite20.Models;

namespace Acczite20.Services
{
    public interface ICurrentSession
    {
        Guid OrganizationId { get; set; }
        string? OrganizationName { get; set; }
        string? JwtToken { get; set; }
        bool IsAuthenticated { get; set; }
        bool IsTrialExpired { get; set; }
        string? Authority { get; set; }
        List<AuthPermission> Permissions { get; set; }
        string? SelectedDatabaseType { get; set; }
        string? ConnectionString { get; set; }
        string? DatabaseName { get; set; }
        string? Username { get; set; }
        int TallyXmlPort { get; set; }
        int TallyOdbcPort { get; set; }
        bool IsTallyConnected { get; set; }
        bool IsConnected { get; }
        void ClearSession();
    }
}
