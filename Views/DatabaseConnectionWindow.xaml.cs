using System;
using System.Windows;
using MySqlConnector;
using Acczite20.Services;
using Acczite20.Views.Pages; // Ensure this matches your actual namespace

namespace Acczite20.Views
{
    public partial class DatabaseConnectionWindow : Window
    {
        private readonly string _dbType;

        public DatabaseConnectionWindow(string dbType)
        {
            InitializeComponent();
            _dbType = dbType;
            DbTypeLabel.Text = $"Enter {_dbType} Credentials";
        }

        // Public property to expose connection string after successful connection
        public string? ConnectionString { get; private set; }

        private void ShowPasswordToggle_Checked(object sender, RoutedEventArgs e)
        {
            PasswordTextBox.Text = PasswordBox.Password;
            PasswordTextBox.Visibility = Visibility.Visible;
            PasswordBox.Visibility = Visibility.Collapsed;
            PasswordTextBox.Focus();
            PasswordTextBox.SelectionStart = PasswordTextBox.Text.Length;
        }

        private void ShowPasswordToggle_Unchecked(object sender, RoutedEventArgs e)
        {
            PasswordBox.Password = PasswordTextBox.Text;
            PasswordBox.Visibility = Visibility.Visible;
            PasswordTextBox.Visibility = Visibility.Collapsed;
            PasswordBox.Focus();
            PasswordBox.SelectAll();
        }

        private void PasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (ShowPasswordToggle.IsChecked == true)
            {
                PasswordTextBox.Text = PasswordBox.Password;
            }
        }

        private void PasswordTextBox_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            if (ShowPasswordToggle.IsChecked != true)
            {
                PasswordBox.Password = PasswordTextBox.Text;
            }
        }

        private void Connect_Click(object sender, RoutedEventArgs e)
        {
            string server = ServerNameBox.Text.Trim();
            string database = DatabaseNameBox.Text.Trim();
            string username = UsernameBox.Text.Trim();
            string password = ShowPasswordToggle.IsChecked == true ? PasswordTextBox.Text : PasswordBox.Password;

            if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(database))
            {
                MessageBox.Show("⚠️ Server and Database name are required.");
                return;
            }

            if (_dbType == "MySQL")
            {
                string connectionString = $"Server={server};Database={database};User={username};Password={password};";

                try
                {
                    using var connection = new MySqlConnection(connectionString);
                    connection.Open(); // Test connection

                    // Store session info
                    SessionManager.Instance.SelectedDatabaseType = _dbType;
                    SessionManager.Instance.ConnectionString = connectionString;

                    ConnectionString = connectionString; // Set property for access from MainWindow

                    MessageBox.Show("✅ Successfully connected to MySQL!");

                    this.DialogResult = true; // Close dialog and signal success
                    this.Close();
                }
                catch (MySqlException ex)
                {
                    MessageBox.Show($"❌ MySQL connection failed:\n{ex.Message}", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"❌ Unexpected error:\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            else
            {
                MessageBox.Show($"⚠️ {_dbType} connection not implemented yet.");
            }
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
            this.Close();
        }
    }
}
