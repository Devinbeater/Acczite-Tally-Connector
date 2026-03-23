using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;

using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public partial class SettingsPage : Page
    {
        private readonly TallyXmlService _tallyService = new();
        private readonly string TallyConfigFile = "tallyconfig.json";
        private readonly INavigationService _navigationService;

        public SettingsPage(INavigationService navigationService)
        {
            _navigationService = navigationService;
            InitializeComponent();
            LoadTallyConfig();
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack) _navigationService.GoBack();
        }

        private void LoadTallyConfig()
        {
            try
            {
                if (File.Exists(TallyConfigFile))
                {
                    var json = File.ReadAllText(TallyConfigFile);
                    var cfg = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                    if (cfg != null)
                    {
                        if (cfg.ContainsKey("XmlPort")) XmlPortBox.Text = cfg["XmlPort"];
                        if (cfg.ContainsKey("OdbcPort")) OdbcPortBox.Text = cfg["OdbcPort"];

                        if (int.TryParse(XmlPortBox.Text, out int xp)) SessionManager.Instance.TallyXmlPort = xp;
                        if (int.TryParse(OdbcPortBox.Text, out int op)) SessionManager.Instance.TallyOdbcPort = op;
                    }
                }
            }
            catch { }
        }

        private void SaveTallyConfig()
        {
            try
            {
                var cfg = new Dictionary<string, string>
                {
                    { "XmlPort", XmlPortBox.Text },
                    { "OdbcPort", OdbcPortBox.Text }
                };
                File.WriteAllText(TallyConfigFile, JsonSerializer.Serialize(cfg));

                if (int.TryParse(XmlPortBox.Text, out int xp)) SessionManager.Instance.TallyXmlPort = xp;
                if (int.TryParse(OdbcPortBox.Text, out int op)) SessionManager.Instance.TallyOdbcPort = op;
            }
            catch { }
        }

        // ── TEST TALLY XML — Full 3-State Detection ──
        private async void TestTallyXml_Click(object sender, RoutedEventArgs e)
        {
            SaveTallyConfig();
            SetStatus("Checking Tally XML API...", "#F59E0B"); // Amber

            var status = await _tallyService.DetectTallyStatusAsync();

            switch (status)
            {
                case TallyConnectionStatus.RunningWithCompany:
                    SetStatus($"✅ Tally Running + Company Loaded (Port {SessionManager.Instance.TallyXmlPort})", "#22C55E");
                    SessionManager.Instance.IsTallyConnected = true;
                    await LoadCompanies();
                    break;

                case TallyConnectionStatus.RunningNoCompany:
                    SetStatus("\u26A0 Tally Running — No Company Open", "#F59E0B");
                    SessionManager.Instance.IsTallyConnected = true;
                    TallyCompaniesPanel.Visibility = Visibility.Collapsed;
                    break;

                case TallyConnectionStatus.NotRunning:
                    SetStatus("\u274C Tally Not Detected (start Tally ERP)", "#EF4444");
                    SessionManager.Instance.IsTallyConnected = false;
                    TallyCompaniesPanel.Visibility = Visibility.Collapsed;
                    break;
            }
        }

        // ── TEST ODBC PORT ──
        private async void TestTallyOdbc_Click(object sender, RoutedEventArgs e)
        {
            SaveTallyConfig();
            SetStatus("Checking Tally ODBC...", "#F59E0B");

            try
            {
                // Note: The default DSN for Tally is usually "TallyODBC". Alternatively, specify Driver details.
                // Depending on the Tally setup, this connection string may need tweaking.
                // Assuming port was specified, "Driver={Tally ODBC Driver};Server=localhost;Port=" + SessionManager.Instance.TallyOdbcPort + ";"
                string odbcConnStr = $"Driver={{Tally ODBC Driver64}};Server=localhost;Port={SessionManager.Instance.TallyOdbcPort};";
                
                using (var conn = new System.Data.Odbc.OdbcConnection(odbcConnStr))
                {
                    await conn.OpenAsync();
                    SetStatus($"✅ ODBC Connect Success (Port {SessionManager.Instance.TallyOdbcPort})", "#22C55E");
                    SessionManager.Instance.IsTallyConnected = true;
                }
            }
            catch (Exception ex)
            {
                // Fallback attempt for 32-bit driver
                try
                {
                    string odbcConnStr32 = $"Driver={{Tally ODBC Driver}};Server=localhost;Port={SessionManager.Instance.TallyOdbcPort};";
                    using (var conn = new System.Data.Odbc.OdbcConnection(odbcConnStr32))
                    {
                        await conn.OpenAsync();
                        SetStatus($"✅ ODBC Connect Success (Port {SessionManager.Instance.TallyOdbcPort})", "#22C55E");
                        SessionManager.Instance.IsTallyConnected = true;
                    }
                }
                catch
                {
                    SetStatus($"❌ ODBC Connection Failed", "#EF4444");
                    SessionManager.Instance.IsTallyConnected = false;
                }
            }
        }

        private async System.Threading.Tasks.Task LoadCompanies()
        {
            TallyCompaniesListBox.Items.Clear();
            var companies = await _tallyService.GetTallyCompaniesAsync();
            if (companies.Count > 0)
            {
                foreach (var c in companies)
                    TallyCompaniesListBox.Items.Add($"\uD83C\uDFE2 {c}");
                TallyCompaniesPanel.Visibility = Visibility.Visible;
            }
            else
            {
                TallyCompaniesListBox.Items.Add("No companies found (open a company in Tally)");
                TallyCompaniesPanel.Visibility = Visibility.Visible;
            }
        }

        private void SetStatus(string text, string color)
        {
            TallyStatusText.Text = text;
            TallyStatusDot.Fill = new SolidColorBrush((Color)ColorConverter.ConvertFromString(color));
        }

        private void TallyCompaniesListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (TallyCompaniesListBox.SelectedItem is string item)
            {
                // Strip the emoji 🏢 and space
                string company = item.Replace("\uD83C\uDFE2 ", "").Trim();
                SessionManager.Instance.TallyCompanyName = company;
                SetStatus($"🎯 Selected Company: {company}", "#22C55E");
            }
        }

        private void EditConnection_Click(object sender, RoutedEventArgs e)
        {
            _navigationService.NavigateTo<DatabaseConnectionPage>();
        }
    }
}
