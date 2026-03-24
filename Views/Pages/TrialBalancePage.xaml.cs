using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;
using Acczite20.Services.Reports;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views.Pages
{
    public class TbRowItem
    {
        public string Name    { get; set; } = string.Empty;
        public string RawName { get; set; } = string.Empty;   // unformatted ledger name for drill-down navigation
        public decimal Debit  { get; set; }
        public decimal Credit { get; set; }
        public bool IsGroup   { get; set; }
    }

    public partial class TrialBalancePage : Page, INotifyPropertyChanged
    {
        private readonly TrialBalanceService _tbService;

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private TrialBalanceReportModel _report = new();
        public TrialBalanceReportModel Report
        {
            get => _report;
            set { _report = value; OnPropertyChanged(); }
        }

        private ObservableCollection<TbRowItem> _flattenedRows = new();
        public ObservableCollection<TbRowItem> FlattenedRows
        {
            get => _flattenedRows;
            set { _flattenedRows = value; OnPropertyChanged(); }
        }

        public string StatusText => Report?.IsBalanced == true ? "Balanced" : $"Difference: {Report?.DifferenceAmount:N2}";
        public string StatusIcon => Report?.IsBalanced == true ? "✓" : "⚠";
        public SolidColorBrush StatusBackgroundBrush => Report?.IsBalanced == true ? new SolidColorBrush(Color.FromRgb(220, 252, 231)) : new SolidColorBrush(Color.FromRgb(254, 242, 242));
        public SolidColorBrush StatusTextBrush => Report?.IsBalanced == true ? new SolidColorBrush(Color.FromRgb(21, 128, 61)) : new SolidColorBrush(Color.FromRgb(185, 28, 28));

        public TrialBalancePage(TrialBalanceService tbService)
        {
            _tbService = tbService;
            InitializeComponent();
            DataContext = this;
            Loaded += TrialBalancePage_Loaded;
        }

        private async void TrialBalancePage_Loaded(object sender, RoutedEventArgs e)
        {
            AsOfDatePicker.SelectedDate = DateTime.Today;
            await LoadTrialBalanceAsync();
        }

        private async void AsOfDatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        {
            await LoadTrialBalanceAsync();
        }

        private async System.Threading.Tasks.Task LoadTrialBalanceAsync()
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty) return;

            // Pass as asOfDate (point-in-time cutoff), not fromDate (period start filter).
            // fromDate = null means: use Tally opening balances + all transactions up to asOfDate.
            DateTimeOffset? asOf = AsOfDatePicker.SelectedDate.HasValue
                ? (DateTimeOffset?)AsOfDatePicker.SelectedDate.Value.Date.AddDays(1).AddTicks(-1)
                : null;

            var result = await _tbService.GenerateTrialBalanceAsync(orgId, asOfDate: asOf);

            Report = result;
            FlattenToDataGrid(result);

            OnPropertyChanged(nameof(StatusText));
            OnPropertyChanged(nameof(StatusIcon));
            OnPropertyChanged(nameof(StatusBackgroundBrush));
            OnPropertyChanged(nameof(StatusTextBrush));
        }

        private void TrialBalanceGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (TrialBalanceGrid.SelectedItem is not TbRowItem row || row.IsGroup || string.IsNullOrEmpty(row.RawName))
                return;

            var asOf = AsOfDatePicker.SelectedDate ?? DateTime.Today;
            int fyYear = asOf.Month >= 4 ? asOf.Year : asOf.Year - 1;
            var from = new DateTimeOffset(fyYear, 4, 1, 0, 0, 0, TimeSpan.Zero);
            var to   = new DateTimeOffset(asOf.Year, asOf.Month, asOf.Day, 23, 59, 59, TimeSpan.Zero);

            var service = App.Current.ServiceProvider.GetRequiredService<LedgerDrillDownService>();
            var page    = new LedgerDrillDownPage(service, row.RawName, from, to);
            NavigationService?.Navigate(page);
        }

        private void FlattenToDataGrid(TrialBalanceReportModel report)
        {
            FlattenedRows.Clear();

            void AddGroup(TrialBalanceGroup group, int depth)
            {
                var indent = new string(' ', depth * 4);
                var icon = depth == 0 ? "📁" : "  └";
                FlattenedRows.Add(new TbRowItem
                {
                    Name = $"{indent}{icon} {group.GroupName}",
                    Debit = group.TotalDebit,
                    Credit = group.TotalCredit,
                    IsGroup = true
                });

                foreach (var ledger in group.Ledgers)
                {
                    var ledgerIndent = new string(' ', (depth + 1) * 4);
                    FlattenedRows.Add(new TbRowItem
                    {
                        Name    = $"{ledgerIndent}• {ledger.LedgerName}",
                        RawName = ledger.LedgerName,
                        Debit   = ledger.DebitBalance,
                        Credit  = ledger.CreditBalance,
                        IsGroup = false
                    });
                }

                // Recurse into child groups
                foreach (var child in group.ChildGroups)
                    AddGroup(child, depth + 1);
            }

            void AddSection(string header, System.Collections.Generic.List<TrialBalanceGroup> groups)
            {
                if (groups.Count == 0) return;
                FlattenedRows.Add(new TbRowItem { Name = header.ToUpper(), IsGroup = true });
                foreach (var group in groups)
                    AddGroup(group, 0);
            }

            AddSection("Assets", report.Assets);
            AddSection("Liabilities", report.Liabilities);
            AddSection("Incomes", report.Incomes);
            AddSection("Expenses", report.Expenses);
        }
    }
}
