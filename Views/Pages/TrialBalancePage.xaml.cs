using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    public class TbRowItem
    {
        public string Name { get; set; } = string.Empty;
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
        public bool IsGroup { get; set; }
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

            var result = await _tbService.GenerateTrialBalanceAsync(
                orgId,
                AsOfDatePicker.SelectedDate
            );

            Report = result;
            FlattenToDataGrid(result);

            OnPropertyChanged(nameof(StatusText));
            OnPropertyChanged(nameof(StatusIcon));
            OnPropertyChanged(nameof(StatusBackgroundBrush));
            OnPropertyChanged(nameof(StatusTextBrush));
        }

        private void FlattenToDataGrid(TrialBalanceReportModel report)
        {
            FlattenedRows.Clear();

            void AddSection(string header, System.Collections.Generic.List<TrialBalanceGroup> groups)
            {
                if (groups.Count == 0) return;

                decimal sectionDebit = 0;
                decimal sectionCredit = 0;

                FlattenedRows.Add(new TbRowItem { Name = header.ToUpper(), IsGroup = true });

                foreach (var group in groups)
                {
                    FlattenedRows.Add(new TbRowItem 
                    { 
                        Name = $"  📁 {group.GroupName}", 
                        Debit = group.TotalDebit, 
                        Credit = group.TotalCredit, 
                        IsGroup = true 
                    });

                    foreach (var ledger in group.Ledgers)
                    {
                        FlattenedRows.Add(new TbRowItem 
                        { 
                            Name = $"       • {ledger.LedgerName}", 
                            Debit = ledger.DebitBalance, 
                            Credit = ledger.CreditBalance, 
                            IsGroup = false 
                        });
                    }

                    sectionDebit += group.TotalDebit;
                    sectionCredit += group.TotalCredit;
                }
            }

            AddSection("Assets", report.Assets);
            AddSection("Liabilities", report.Liabilities);
            AddSection("Incomes", report.Incomes);
            AddSection("Expenses", report.Expenses);
        }
    }
}
