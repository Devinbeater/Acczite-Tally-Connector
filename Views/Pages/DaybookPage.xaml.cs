using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    public partial class DaybookPage : Page
    {
        private readonly DaybookService _daybookService;
        private readonly SessionManager _sessionManager;

        private int _currentPage = 1;
        private const int PageSize = 100;
        private PagedDaybookResult? _lastResult;

        private DateTime _lastFrom     = DateTime.MinValue;
        private DateTime _lastTo       = DateTime.MinValue;
        private decimal  _cachedDebit;
        private decimal  _cachedCredit;

        public ObservableCollection<DaybookItem> DaybookItems { get; } = new();

        public DaybookPage(DaybookService daybookService, SessionManager sessionManager)
        {
            _daybookService = daybookService;
            _sessionManager = sessionManager;
            InitializeComponent();
            DaybookGrid.ItemsSource = DaybookItems;

            var now = DateTime.Now;
            FromDatePicker.SelectedDate = new DateTime(now.Year, now.Month, 1);
            ToDatePicker.SelectedDate   = now;
        }

        private async void Page_Loaded(object sender, RoutedEventArgs e)
            => await LoadPageAsync(1);

        private async void Refresh_Click(object sender, RoutedEventArgs e)
            => await LoadPageAsync(1);

        private async void BtnPrev_Click(object sender, RoutedEventArgs e)
        {
            if (_currentPage > 1) await LoadPageAsync(_currentPage - 1);
        }

        private async void BtnNext_Click(object sender, RoutedEventArgs e)
        {
            if (_lastResult?.HasNext == true) await LoadPageAsync(_currentPage + 1);
        }

        private async Task LoadPageAsync(int page)
        {
            try
            {
                LoadingRing.IsActive        = true;
                LoadingRing.Visibility      = Visibility.Visible;
                DaybookGrid.Visibility      = Visibility.Collapsed;
                EmptyStatePanel.Visibility  = Visibility.Collapsed;
                PaginationBar.Visibility    = Visibility.Collapsed;
                TotalsBar.Visibility        = Visibility.Collapsed;

                var orgId = _sessionManager.OrganizationId;
                if (orgId == Guid.Empty) { EmptyStatePanel.Visibility = Visibility.Visible; return; }

                var from = FromDatePicker.SelectedDate ?? new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1);
                var to   = ToDatePicker.SelectedDate   ?? DateTime.Now;

                bool sameRange = from == _lastFrom && to == _lastTo;
                var result = await _daybookService.GetPagedAsync(orgId, from, to, page, PageSize,
                    skipPeriodTotals: sameRange);
                _lastResult  = result;
                _currentPage = result.Page;

                DaybookItems.Clear();
                foreach (var r in result.Rows)
                {
                    DaybookItems.Add(new DaybookItem
                    {
                        VoucherDate     = r.VoucherDate,
                        VoucherNumber   = r.VoucherNumber,
                        VoucherType     = r.VoucherType,
                        MainLedgerName  = r.PartyLedgerName,
                        Narration       = r.Narration,
                        DebitAmount     = r.DebitAmount,
                        CreditAmount    = r.CreditAmount,
                    });
                }

                if (DaybookItems.Count == 0)
                {
                    EmptyStatePanel.Visibility = Visibility.Visible;
                    return;
                }

                DaybookGrid.Visibility = Visibility.Visible;

                // Pagination bar
                TxtPageInfo.Text = $"Page {result.Page} of {result.TotalPages}  ({result.TotalCount:N0} entries)";
                BtnPrev.IsEnabled = result.HasPrevious;
                BtnNext.IsEnabled = result.HasNext;
                PaginationBar.Visibility = Visibility.Visible;

                // Totals footer — update cache only when date range changed
                if (!sameRange)
                {
                    _cachedDebit  = result.PeriodTotalDebit;
                    _cachedCredit = result.PeriodTotalCredit;
                    _lastFrom     = from;
                    _lastTo       = to;
                }
                TxtTotalDebit.Text  = _cachedDebit.ToString("N2");
                TxtTotalCredit.Text = _cachedCredit.ToString("N2");
                TotalsBar.Visibility = Visibility.Visible;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load daybook: {ex.Message}", "Error",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                LoadingRing.IsActive    = false;
                LoadingRing.Visibility  = Visibility.Collapsed;
            }
        }
    }

    public class DaybookItem
    {
        public DateTime VoucherDate    { get; set; }
        public string   VoucherNumber  { get; set; } = string.Empty;
        public string   VoucherType    { get; set; } = string.Empty;
        public string   MainLedgerName { get; set; } = string.Empty;
        public string   Narration      { get; set; } = string.Empty;
        public decimal  DebitAmount    { get; set; }
        public decimal  CreditAmount   { get; set; }

        public string DebitDisplay  => DebitAmount  > 0 ? DebitAmount.ToString("N2")  : string.Empty;
        public string CreditDisplay => CreditAmount > 0 ? CreditAmount.ToString("N2") : string.Empty;
    }
}
