using System.Windows;
using Acczite20.Services;

namespace Acczite20.Views
{
    public partial class DatabaseTypeWindow : Window
    {
        public DatabaseTypeWindow()
        {
            InitializeComponent();
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            string? selectedType = null;

            if (SqlServerRadio.IsChecked == true)
                selectedType = "SQL Server";
            else if (PostgreSqlRadio.IsChecked == true)
                selectedType = "PostgreSQL";
            else if (SqliteRadio.IsChecked == true)
                selectedType = "SQLite";
            else if (MySqlRadio.IsChecked == true)
                selectedType = "MySQL";

            if (string.IsNullOrEmpty(selectedType))
            {
                MessageBox.Show("⚠️ Please select a database type before proceeding.", "Missing Selection", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // 🔐 Store selected database type
            SessionManager.Instance.SelectedDatabaseType = selectedType;

            // 🔄 Open connection window only if not already connected
            if (!SessionManager.Instance.IsConnected)
            {
                var connectionWindow = new DatabaseConnectionWindow(selectedType)
                {
                    Owner = this,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner
                };

                bool? result = connectionWindow.ShowDialog();

                if (result != true || !SessionManager.Instance.IsConnected)
                {
                    MessageBox.Show("❌ Database connection setup failed or was cancelled.",
                                    "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }
            }

            this.DialogResult = true;
            this.Close();
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
            this.Close();
        }
    }
}
