using System;

namespace Acczite20
{
    /// <summary>
    /// Global application state for tracking user and license information.
    /// </summary>
    public static class AppState
    {
        /// <summary>
        /// Indicates if a user is currently logged in.
        /// </summary>
        public static bool IsLoggedIn { get; set; } = false;

        /// <summary>
        /// Stores the username of the logged-in user.
        /// </summary>
        public static string LoggedInUser { get; set; } = string.Empty;

        /// <summary>
        /// Stores the currently logged-in user's display name or username (for UI use).
        /// </summary>
        public static string CurrentUsername { get; set; } = string.Empty;

        /// <summary>
        /// Indicates whether the license key is valid.
        /// </summary>
        public static bool IsLicenseValid { get; set; } = false;

        /// <summary>
        /// Stores the license expiry date.
        /// </summary>
        public static DateTime LicenseExpiryDate { get; set; } = DateTime.Now.AddDays(30); // Default: 30 days from now

        /// <summary>
        /// Calculates the number of days remaining until license expiry.
        /// </summary>
        public static int DaysRemaining => (LicenseExpiryDate - DateTime.Now).Days;

        /// <summary>
        /// Gets the current version of the application.
        /// </summary>
        public static string AppVersion { get; } = "1.0.0"; // Update as needed
    }
}
