using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services.Navigation;
using Microsoft.Data.SqlClient;
using MongoDB.Bson;
using MongoDB.Driver;
using MySqlConnector;

namespace Acczite20.Views.Pages
{
    public partial class DatabaseConnectionPage : Page
    {
        private readonly string ConfigFile = "dbconfig.json";
        private readonly INavigationService _navigationService;
        private readonly Services.MongoService _mongoService;

        public DatabaseConnectionPage(INavigationService navigationService, Services.MongoService mongoService)
        {
            _navigationService = navigationService;
            _mongoService = mongoService;
            InitializeComponent();
            Loaded += DatabaseConnectionPage_Loaded;
        }

        private void DatabaseConnectionPage_Loaded(object sender, RoutedEventArgs e)
        {
            LoadConfig();
            UpdatePanelVisibility();
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (NavigationService?.CanGoBack == true) NavigationService.GoBack();
        }

        private void DbTypeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            UpdatePanelVisibility();
        }

        private void UpdatePanelVisibility()
        {
            if (SqlPanel == null || MongoPanel == null || DbTypeComboBox == null || CollectionsPanel == null) return;

            CollectionsPanel.Visibility = Visibility.Collapsed;

            if (DbTypeComboBox.Text == "MongoDB"
                || (DbTypeComboBox.SelectedItem is ComboBoxItem item && item.Content.ToString() == "MongoDB"))
            {
                SqlPanel.Visibility = Visibility.Collapsed;
                MongoPanel.Visibility = Visibility.Visible;
                CollectionsHeader.Text = "Available MongoDB Collections";
            }
            else
            {
                SqlPanel.Visibility = Visibility.Visible;
                MongoPanel.Visibility = Visibility.Collapsed;
                CollectionsHeader.Text = "Available SQL Tables";
            }
        }

        private void LoadConfig()
        {
            try
            {
                if (File.Exists(ConfigFile))
                {
                    var configJson = File.ReadAllText(ConfigFile);
                    var config = JsonSerializer.Deserialize<Dictionary<string, string>>(configJson);

                    if (config != null)
                    {
                        if (config.ContainsKey("DbType")) DbTypeComboBox.Text = config["DbType"];

                        if (config.ContainsKey("Server")) ServerBox.Text = config["Server"];
                        if (config.ContainsKey("Port")) PortBox.Text = config["Port"];
                        if (config.ContainsKey("Database")) DatabaseBox.Text = config["Database"];
                        if (config.ContainsKey("Username")) UsernameBox.Text = config["Username"];
                        if (config.ContainsKey("Password")) PasswordBox.Password = config["Password"];

                        if (config.ContainsKey("MongoUri")) MongoUriBox.Text = config["MongoUri"];
                    }
                }
            }
            catch
            {
            }
        }

        private void SaveConfig()
        {
            try
            {
                var config = new Dictionary<string, string>
                {
                    { "DbType", DbTypeComboBox.Text },
                    { "Server", ServerBox.Text },
                    { "Port", PortBox.Text },
                    { "Database", DatabaseBox.Text },
                    { "Username", UsernameBox.Text },
                    { "Password", PasswordBox.Password },
                    { "MongoUri", MongoUriBox.Text }
                };

                var configJson = JsonSerializer.Serialize(config);
                File.WriteAllText(ConfigFile, configJson);
            }
            catch
            {
            }
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            bool isMongo = DbTypeComboBox.Text == "MongoDB"
                || (DbTypeComboBox.SelectedItem is ComboBoxItem item && item.Content.ToString() == "MongoDB");

            if (isMongo)
            {
                if (string.IsNullOrWhiteSpace(MongoUriBox.Text))
                {
                    MessageBox.Show("Please provide a valid MongoDB Connection URI.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }
            }
            else
            {
                if (string.IsNullOrWhiteSpace(ServerBox.Text) || string.IsNullOrWhiteSpace(DatabaseBox.Text))
                {
                    MessageBox.Show("Please fill out the Server and Database Name.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }
            }

            SaveConfig();
            _navigationService.NavigateTo(new TallySyncPage(new List<string>(), _navigationService));
        }

        private async void TestConnection_Click(object sender, RoutedEventArgs e)
        {
            bool isMongo = DbTypeComboBox.Text == "MongoDB"
                || (DbTypeComboBox.SelectedItem is ComboBoxItem item && item.Content.ToString() == "MongoDB");
            CollectionsListBox.Items.Clear();

            try
            {
                if (isMongo)
                {
                    var client = new MongoClient(MongoUriBox.Text);
                    var db = client.GetDatabase("admin");
                    await db.RunCommandAsync((Command<BsonDocument>)"{ping:1}");

                    CollectionsListBox.Items.Add("✅ Connected to MongoDB Successfully");

                    string targetDb = DatabaseBox.Text;
                    if (string.IsNullOrWhiteSpace(targetDb))
                    {
                        targetDb = "acczite_master";
                    }

                    var targetDatabase = client.GetDatabase(targetDb);
                    var collections = await targetDatabase.ListCollectionNamesAsync();
                    var list = Services.MongoService.FilterRelevantCollections(await collections.ToListAsync());

                    if (list.Any())
                    {
                        CollectionsListBox.Items.Add($"--- Collections in {targetDb} ---");
                        foreach (var col in list)
                        {
                            CollectionsListBox.Items.Add($"\uD83D\uDCC1 {col}");
                        }
                    }
                    else
                    {
                        CollectionsListBox.Items.Add("ℹ No supported accounting, HR, payroll, products, or inventory collections found.");
                    }
                }
                else
                {
                    var engine = DbTypeComboBox.Text;
                    if (engine == "SQL Server")
                    {
                        var connStr = $"Server={ServerBox.Text};Database={DatabaseBox.Text};User Id={UsernameBox.Text};Password={PasswordBox.Password};TrustServerCertificate=True;";
                        using (var conn = new SqlConnection(connStr))
                        {
                            await conn.OpenAsync();
                            var cmd = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn);
                            var reader = await cmd.ExecuteReaderAsync();
                            bool hasTables = false;
                            while (await reader.ReadAsync())
                            {
                                CollectionsListBox.Items.Add($"\uD83D\uDCC1 {reader.GetString(0)}");
                                hasTables = true;
                            }

                            if (!hasTables)
                            {
                                CollectionsListBox.Items.Add("No Tables Found.");
                            }
                        }
                    }
                    else if (engine == "MySQL")
                    {
                        var connStr = $"Server={ServerBox.Text};Port={(string.IsNullOrWhiteSpace(PortBox.Text) ? "3306" : PortBox.Text)};Database={DatabaseBox.Text};Uid={UsernameBox.Text};Pwd={PasswordBox.Password};";
                        using (var conn = new MySqlConnection(connStr))
                        {
                            await conn.OpenAsync();
                            var cmd = new MySqlCommand("SHOW TABLES", conn);
                            var reader = await cmd.ExecuteReaderAsync();
                            bool hasTables = false;
                            while (await reader.ReadAsync())
                            {
                                CollectionsListBox.Items.Add($"\uD83D\uDCC1 {reader.GetString(0)}");
                                hasTables = true;
                            }

                            if (!hasTables)
                            {
                                CollectionsListBox.Items.Add("No Tables Found.");
                            }
                        }
                    }
                    else
                    {
                        CollectionsListBox.Items.Add("Connection test skipped for " + engine);
                    }
                }

                CollectionsPanel.Visibility = Visibility.Visible;
                MessageBox.Show("Connection established successfully! Mappings populated.", "Connection Test", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                CollectionsPanel.Visibility = Visibility.Collapsed;
                string msg = ex.Message;
                if (msg.Contains("mongodb://") || msg.Contains("Server="))
                {
                    msg = "Invalid connection details provided.";
                }

                MessageBox.Show($"Connection failed:\n\n{msg}", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }
}
