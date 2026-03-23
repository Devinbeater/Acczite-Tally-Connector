using System;
using System.Collections.Generic;
using Acczite20.Models;

namespace Acczite20.Services
{
    /// <summary>
    /// Manages session-level data for the currently logged-in user.
    /// Everything here is scoped to a single organization context.
    /// </summary>
    public class SessionManager : ICurrentSession
    {
        public static SessionManager Instance { get; } = new SessionManager();

        // ── Organization Context ──
        public Guid OrganizationId { get; set; } = Guid.Empty;
        public string OrganizationObjectId { get; set; } = string.Empty;
        public string? OrganizationName { get; set; }
        public Guid UserId { get; set; }
        public string UserObjectId { get; set; } = string.Empty;
        public string? JwtToken { get; set; }
        public bool IsAuthenticated { get; set; }
        public bool IsTrialExpired { get; set; }
        public string? Authority { get; set; }
        public List<AuthPermission> Permissions { get; set; } = new List<AuthPermission>();

        // ── Database Context ──
        public string? SelectedDatabaseType { get; set; }
        public string? ConnectionString { get; set; }
        public string? DatabaseName { get; set; }
        public string? Username { get; set; }

        // ── Tally Context ──
        public int TallyXmlPort { get; set; } = 9000;
        public int TallyOdbcPort { get; set; } = 9000;
        public bool IsTallyConnected { get; set; }
        public string? TallyCompanyName { get; set; }
        public int SyncDirection { get; set; } = 0; // 0: Tally->DB, 1: DB->Tally, 2: Two-Way

        public bool IsConnected => !string.IsNullOrWhiteSpace(ConnectionString);

        public void ClearSession()
        {
            OrganizationId = Guid.Empty;
            OrganizationObjectId = string.Empty;
            OrganizationName = null;
            UserId = Guid.Empty;
            UserObjectId = string.Empty;
            JwtToken = null;
            IsAuthenticated = false;
            IsTrialExpired = false;
            Authority = null;
            Permissions.Clear();
            SelectedDatabaseType = null;
            ConnectionString = null;
            DatabaseName = null;
            Username = null;
            TallyXmlPort = 9000;
            TallyOdbcPort = 9000;
            IsTallyConnected = false;
            TallyCompanyName = null;
        }
    }
}
