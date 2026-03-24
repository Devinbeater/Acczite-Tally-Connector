using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Animation;
using Acczite20.Models;
using Acczite20.Services;
using Acczite20.ViewModels.Wizard;
using MongoDB.Bson;
using MongoDB.Driver;
using Acczite20.Services.Navigation;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views.Pages
{
    public partial class TallySyncPage : Page
    {
        private readonly TallySyncViewModel _viewModel;
        private readonly List<string> _selectedTableNames;
        private readonly INavigationService _navigationService;
        private readonly List<string> _allDbCollections = new();
        private System.ComponentModel.ICollectionView? _tallyView;

        public TallySyncPage(List<string> selectedTableNames, INavigationService navigationService)
        {
            InitializeComponent();
            _selectedTableNames = selectedTableNames;
            _navigationService = navigationService;
            var tallyService = ((App)Application.Current).ServiceProvider.GetRequiredService<TallyXmlService>(); _viewModel = new TallySyncViewModel(tallyService);
            DataContext = _viewModel;

            // Initialize filtering for Tally collections
            _tallyView = System.Windows.Data.CollectionViewSource.GetDefaultView(_viewModel.TallyFields);
            _tallyView.Filter = TallyFilter;

            Loaded += TallySyncPage_Loaded;
            SyncDirectionBox.SelectionChanged += SyncDirectionBox_SelectionChanged;
        }

        private bool TallyFilter(object obj)
        {
            if (string.IsNullOrWhiteSpace(TallySearchBox?.Text)) return true;
            if (obj is SelectableItem item)
            {
                return item.Name?.Contains(TallySearchBox.Text, StringComparison.OrdinalIgnoreCase) ?? false;
            }
            return true;
        }

        private void TallySearchBox_TextChanged(object sender, ModernWpf.Controls.AutoSuggestBoxTextChangedEventArgs e)
        {
            _tallyView?.Refresh();
        }

        private void DbSearchBox_TextChanged(object sender, ModernWpf.Controls.AutoSuggestBoxTextChangedEventArgs e)
        {
            ApplyDbFilter();
        }

        private void ApplyDbFilter()
        {
            var filter = DbSearchBox.Text;
            var selected = DbCollectionsListBox.SelectedItems.Cast<string>().Where(IsDbCollectionEntry).ToList();
            
            DbCollectionsListBox.Items.Clear();
            foreach (var col in _allDbCollections)
            {
                if (!IsDbCollectionEntry(col) || string.IsNullOrWhiteSpace(filter) || col.Contains(filter, StringComparison.OrdinalIgnoreCase))
                {
                    DbCollectionsListBox.Items.Add(col);
                }
            }

            // Restore selection if possible
            foreach (var item in selected)
            {
                if (DbCollectionsListBox.Items.Contains(item))
                {
                    DbCollectionsListBox.SelectedItems.Add(item);
                }
            }
        }

        private static bool IsDbCollectionEntry(object? item)
        {
            return item is string value && value.StartsWith("📁 ", StringComparison.Ordinal);
        }

        private void SelectAllTally_Checked(object sender, RoutedEventArgs e)
        {
            foreach (var item in _viewModel.TallyFields)
                item.IsSelected = true;
        }

        private void SelectAllTally_Unchecked(object sender, RoutedEventArgs e)
        {
            foreach (var item in _viewModel.TallyFields)
                item.IsSelected = false;
        }

        private void SelectAllDbCollections()
        {
            DbCollectionsListBox.SelectedItems.Clear();

            foreach (var item in DbCollectionsListBox.Items)
            {
                if (IsDbCollectionEntry(item))
                {
                    DbCollectionsListBox.SelectedItems.Add(item);
                }
            }
        }

        private async void TallySyncPage_Loaded(object sender, RoutedEventArgs e)
        {
            // Auto-load Tally collections
            await LoadTallyCollections();

            // Auto-load DB collections if config exists
            if (File.Exists("dbconfig.json"))
            {
                await LoadDbCollectionsInternal();
            }
        }

        // ── SYNC DIRECTION ARROW UPDATE ──
        private void SyncDirectionBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (SyncArrow == null) return;

            switch (SyncDirectionBox.SelectedIndex)
            {
                case 0: SyncArrow.Text = "→"; SyncLabel.Text = "Tally → DB"; break;
                case 1: SyncArrow.Text = "←"; SyncLabel.Text = "DB → Tally"; break;
                case 2: SyncArrow.Text = "↔"; SyncLabel.Text = "Two-Way"; break;
            }

            SessionManager.Instance.SyncDirection = SyncDirectionBox.SelectedIndex;
        }

        // ── LOAD DB COLLECTIONS ──
        private async void LoadDbCollections_Click(object sender, RoutedEventArgs e)
        {
            await LoadDbCollectionsInternal();
        }

        private async Task LoadDbCollectionsInternal()
        {
            _allDbCollections.Clear();
            DbCollectionsListBox.Items.Clear();
            DbLoadingPanel.Visibility = Visibility.Visible;
            StartDbSpinner();

            try
            {
                if (!File.Exists("dbconfig.json"))
                {
                    _allDbCollections.Add("No database configured. Go to Settings.");
                    ApplyDbFilter();
                    return;
                }

                var json = File.ReadAllText("dbconfig.json");
                var config = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                if (config == null) return;

                var dbType = config.ContainsKey("DbType") ? config["DbType"] : "";

                if (dbType == "MongoDB")
                {
                    DbPanelHeader.Text = "MongoDB Connection";
                    if (!config.TryGetValue("MongoUri", out string? uri) || string.IsNullOrWhiteSpace(uri))
                    {
                        _allDbCollections.Add("🔴 No MongoDB connection configured.");
                        ApplyDbFilter();
                        return;
                    }

                    var serviceProvider = ((App)Application.Current).ServiceProvider;
                    var mongoService = serviceProvider.GetRequiredService<MongoService>();
                    var db = mongoService.GetDatabase();

                    await db.RunCommandAsync((Command<BsonDocument>)"{ping:1}");

                    _allDbCollections.Add("🟢 Mongo Connected");
                    
                    var collections = MongoService.FilterRelevantCollections(await mongoService.ListCollectionsAsync());
                    if (collections.Count > 0)
                    {
                        foreach (var coll in collections)
                        {
                            _allDbCollections.Add($"📁 {coll}");
                        }
                    }
                    else
                    {
                        _allDbCollections.Add("ℹ No supported accounting, HR, payroll, products, or inventory collections found.");
                    }
                }
                else if (dbType == "SQL Server")
                {
                    DbPanelHeader.Text = "SQL Server Tables";
                    var server = config.ContainsKey("Server") ? config["Server"] : "";
                    var database = config.ContainsKey("Database") ? config["Database"] : "";
                    var username = config.ContainsKey("Username") ? config["Username"] : "";
                    var password = config.ContainsKey("Password") ? config["Password"] : "";
                    var connStr = $"Server={server};Database={database};User Id={username};Password={password};TrustServerCertificate=True;";

                    using var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
                    await conn.OpenAsync();
                    var cmd = new Microsoft.Data.SqlClient.SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", conn);
                    var reader = await cmd.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                        _allDbCollections.Add($"📁 {reader.GetString(0)}");
                }
                else if (dbType == "MySQL")
                {
                    DbPanelHeader.Text = "MySQL Tables";
                    var server = config.ContainsKey("Server") ? config["Server"] : "";
                    var port = config.ContainsKey("Port") ? config["Port"] : "3306";
                    var database = config.ContainsKey("Database") ? config["Database"] : "";
                    var username = config.ContainsKey("Username") ? config["Username"] : "";
                    var password = config.ContainsKey("Password") ? config["Password"] : "";
                    var connStr = $"Server={server};Port={port};Database={database};Uid={username};Pwd={password};";

                    using var conn = new MySqlConnector.MySqlConnection(connStr);
                    await conn.OpenAsync();
                    var cmd = new MySqlConnector.MySqlCommand("SHOW TABLES", conn);
                    var reader = await cmd.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                        _allDbCollections.Add($"📁 {reader.GetString(0)}");
                }
                else
                {
                    _allDbCollections.Add($"Unsupported DB: {dbType}");
                }

                ApplyDbFilter();
                SelectAllDbCollections();
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
                if (msg.Contains("mongodb://") || msg.Contains("Server="))
                {
                    msg = "Connection failed. Please check your DB settings.";
                }
                _allDbCollections.Add($"❌ {msg}");
                ApplyDbFilter();
            }
            finally
            {
                DbLoadingPanel.Visibility = Visibility.Collapsed;
            }
        }

        // ── LOAD TALLY COLLECTIONS ──
        private async Task LoadTallyCollections()
        {
            TallyLoadingPanel.Visibility = Visibility.Visible;
            TallyFieldsListBox.Visibility = Visibility.Collapsed;
            StartTallySpinner();

            await _viewModel.LoadCollectionsFromTallyAsync();

            TallyLoadingPanel.Visibility = Visibility.Collapsed;
            TallyFieldsListBox.Visibility = Visibility.Visible;

            if (_viewModel.IsTallyConnected)
                TallyDot.Fill = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#22C55E"));
            else
                TallyDot.Fill = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#EF4444"));

            foreach (var item in _viewModel.TallyFields)
                item.SelectionChanged += TallyField_SelectionChanged;
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadTallyCollections();
        }

        // ── SPINNER HELPERS ──
        private void StartTallySpinner()
        {
            var storyboard = new Storyboard();
            var animation = new DoubleAnimation { From = 0, To = 360, Duration = TimeSpan.FromSeconds(1), RepeatBehavior = RepeatBehavior.Forever };
            Storyboard.SetTarget(animation, SpinnerRotate);
            Storyboard.SetTargetProperty(animation, new PropertyPath(RotateTransform.AngleProperty));
            storyboard.Children.Add(animation);
            storyboard.Begin();
        }

        private void StartDbSpinner()
        {
            var storyboard = new Storyboard();
            var animation = new DoubleAnimation { From = 0, To = 360, Duration = TimeSpan.FromSeconds(1), RepeatBehavior = RepeatBehavior.Forever };
            Storyboard.SetTarget(animation, DbSpinnerRotate);
            Storyboard.SetTargetProperty(animation, new PropertyPath(RotateTransform.AngleProperty));
            storyboard.Children.Add(animation);
            storyboard.Begin();
        }

        // ── SELECTION TRACKING ──
        private void TallyField_SelectionChanged(object? sender, EventArgs e)
        {
            int order = 1;
            foreach (var item in _viewModel.TallyFields.Where(x => x.IsSelected))
                item.SequenceNumber = order++;
            foreach (var item in _viewModel.TallyFields.Where(x => !x.IsSelected))
                item.SequenceNumber = 0;
        }

        private async void PreviewFields_Click(object sender, RoutedEventArgs e)
        {
            var selected = _viewModel.TallyFields.FirstOrDefault(f => f.IsSelected);
            if (selected == null)
            {
                MessageBox.Show("Select at least one Tally collection to preview.", "No Selection", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var fields = await _viewModel.LoadFieldsForCollectionAsync(selected.Name!);
            var msg = fields.Count > 0
                ? $"Fields in \"{selected.Name}\":\n\n• {string.Join("\n• ", fields.Take(30))}"
                : "No fields returned. Ensure a company is open in Tally.";

            MessageBox.Show(msg, $"Field Preview — {selected.Name}", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (NavigationService?.CanGoBack == true) NavigationService.GoBack();
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            var selectedFields = _viewModel.GetSelectedFields();
            if (selectedFields.Count == 0)
            {
                MessageBox.Show("Select at least one Tally collection before continuing.", "No Selection", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Get sync direction
            var direction = SyncDirectionBox.SelectedIndex switch
            {
                0 => "tally_to_db",
                1 => "db_to_tally",
                2 => "bidirectional",
                _ => "tally_to_db"
            };

            // Get selected DB collections
            var dbTables = new List<string>();
            foreach (var item in DbCollectionsListBox.SelectedItems)
            {
                if (IsDbCollectionEntry(item))
                {
                    dbTables.Add(item?.ToString()?.Replace("📁 ", "") ?? "");
                }
            }

            _navigationService.NavigateTo(new TallyExecutePage(dbTables, selectedFields, _navigationService));
        }
    }
}
