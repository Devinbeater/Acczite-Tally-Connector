using System;
using System.ComponentModel.DataAnnotations;

namespace Acczite20.Models
{
/// <summary>
/// Represents a registered user in the system.
/// </summary>
public class User
{
    /// <summary>
    /// Primary key of the user.
    /// </summary>
    public int Id { get; set; }

    /// <summary>
    /// Full name of the user.
    /// </summary>
    [Required]
    [MaxLength(100)]
    public string FullName { get; set; } = string.Empty;

    /// <summary>
    /// Email address of the user.
    /// </summary>
    [Required]
    [EmailAddress]
    [MaxLength(100)]
    public string Email { get; set; } = string.Empty;

    /// <summary>
    /// Mobile number of the user.
    /// </summary>
    [Required]
    [Phone]
    [MaxLength(15)]
    public string Mobile { get; set; } = string.Empty;

    /// <summary>
    /// Username used for login.
    /// </summary>
    [Required]
    [MaxLength(50)]
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// Hashed password for security.
    /// </summary>
    [Required]
    public string PasswordHash { get; set; } = string.Empty;

    /// <summary>
    /// Salt used for hashing the password.
    /// </summary>
    [Required]
    public string Salt { get; set; } = string.Empty;

    /// <summary>
    /// Indicates whether the account is active.
    /// </summary>
    public bool IsActive { get; set; } = true;

    /// <summary>
    /// Indicates whether the user is a Tally user.
    /// </summary>
    public bool IsTallyUser { get; set; } = false;

    /// <summary>
    /// The date the user registered.
    /// </summary>
    public DateTime RegistrationDate { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Renewal type (e.g., Monthly, Yearly, Trial).
    /// </summary>
    [MaxLength(50)]
    public string? RenewalType { get; set; } = null;
}
}
