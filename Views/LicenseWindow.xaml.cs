using System;
using System.Windows;
using MySqlConnector;

namespace Acczite20.Views
{
    public partial class LicenseWindow : Window
    {
        public LicenseWindow()
        {
            InitializeComponent();
            // Initially disable Next button until license is verified
            NextButton.IsEnabled = false;
        }

        private async void Verify_Click(object sender, RoutedEventArgs e)
        {
            string licenseKey = LicenseKeyBox.Text.Trim();

            if (string.IsNullOrEmpty(licenseKey))
            {
                MessageBox.Show("Please enter a license key.", "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                // Optionally disable controls during verification to prevent double-clicks
                VerifyButton.IsEnabled = false;
                LicenseKeyBox.IsEnabled = false;

                bool isValid = await IsLicenseValidAsync(licenseKey);

                if (isValid)
                {
                    MessageBox.Show("License key verified successfully!", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    NextButton.IsEnabled = true;
                }
                else
                {
                    MessageBox.Show("Invalid or expired license key. Please try again.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    VerifyButton.IsEnabled = true;
                    LicenseKeyBox.IsEnabled = true;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"License verification failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                VerifyButton.IsEnabled = true;
                LicenseKeyBox.IsEnabled = true;
            }
        }

        private async System.Threading.Tasks.Task<bool> IsLicenseValidAsync(string licenseKey)
        {
            string connectionString = "Server=103.120.176.151;Database=biztech_acczite;User=biztech_acczite;Password=Gaurav@2709;";
            using var connection = new MySqlConnection(connectionString);

            await connection.OpenAsync();

            string query = @"
                SELECT COUNT(*) FROM licenses 
                WHERE license_key = @licenseKey 
                  AND is_active = 1 
                  AND expires_at > NOW()";

            using var command = new MySqlCommand(query, connection);
            command.Parameters.AddWithValue("@licenseKey", licenseKey);

            var result = await command.ExecuteScalarAsync();

            return Convert.ToInt32(result) > 0;
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
            this.Close();
        }

        private void Minimize_Click(object sender, RoutedEventArgs e)
        {
            WindowState = WindowState.Minimized;
        }

        private void CloseChrome_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }
    }
}
