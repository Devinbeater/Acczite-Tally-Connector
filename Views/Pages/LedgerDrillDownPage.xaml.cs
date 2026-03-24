using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    internal class LedgerEntryRowVm
    {
        private readonly LedgerEntryRow _row;

        public LedgerEntryRowVm(LedgerEntryRow row) => _row = row;

        public DateTimeOffset Date         => _row.Date;
        public string VoucherNumber        => _row.VoucherNumber;
        public string VoucherType          => _row.VoucherType;
        public string Narration            => _row.Narration;
        public decimal RunningBalance      => _row.RunningBalance;
        public Guid VoucherId              => _row.VoucherId;

        public string DebitDisplay  => _row.Debit  == 0m ? string.Empty : $"₹ {_row.Debit:N2}";
        public string CreditDisplay => _row.Credit == 0m ? string.Empty : $"₹ {_row.Credit:N2}";

        public string BalanceDisplay => _row.RunningBalance >= 0
            ? $"₹ {_row.RunningBalance:N2} Dr"
            : $"₹ {Math.Abs(_row.RunningBalance):N2} Cr";

        public bool IsNegativeBalance => _row.RunningBalance < 0;
    }

    public partial class LedgerDrillDownPage : Page
    {
        private readonly LedgerDrillDownService _service;
        private readonly string _ledgerName;
        private readonly DateTimeOffset _from;
        private readonly DateTimeOffset _to;
        private int _currentPage = 1;

        public LedgerDrillDownPage(
            LedgerDrillDownService service,
            string ledgerName,
            DateTimeOffset from,
            DateTimeOffset to)
        {
            InitializeComponent();
            _service    = service;
            _ledgerName = ledgerName;
            _from       = from;
            _to         = to;
            Loaded += async (_, _) => await LoadPageAsync(1);
        }

        private async Task LoadPageAsync(int page)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty) return;

            try
            {
                var result = await _service.GetAsync(orgId, _ledgerName, _from, _to, page);
                _currentPage = result.Page;

                // ── Header ────────────────────────────────────────────────────────
                TxtLedgerName.Text       = result.LedgerName;
                TxtPeriod.Text           = $"{result.PeriodFrom:dd-MMM-yy}  →  {result.PeriodTo:dd-MMM-yy}";
                TxtOpeningBalance.Text   = FormatBalance(result.OpeningBalance);
                TxtTotalEntries.Text     = result.TotalCount.ToString("N0");

                // ── Page opening carry-forward bar ────────────────────────────────
                if (page > 1)
                {
                    PageOpeningBar.Visibility = Visibility.Visible;
                    TxtPageOpening.Text = $"Opening balance carried forward into this page:  " +
                                          FormatBalance(result.RunningOpeningBalance);
                }
                else
                {
                    PageOpeningBar.Visibility = Visibility.Collapsed;
                }

                // ── DataGrid ──────────────────────────────────────────────────────
                EntriesGrid.ItemsSource = result.Rows
                    .Select(r => new LedgerEntryRowVm(r))
                    .ToList();

                // ── Pagination ────────────────────────────────────────────────────
                TxtPageInfo.Text   = $"Page {result.Page} of {result.TotalPages}";
                TxtEntryRange.Text = result.TotalCount == 0
                    ? "No entries"
                    : $"Showing {result.FirstRowNumber}–{result.LastRowNumber} of {result.TotalCount} entries";

                BtnPrev.IsEnabled = result.HasPrevious;
                BtnNext.IsEnabled = result.HasNext;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load ledger entries:\n{ex.Message}", "Error");
            }
        }

        private static string FormatBalance(decimal amount)
        {
            if (amount == 0m) return "₹ 0.00";
            return amount >= 0
                ? $"₹ {amount:N2} Dr"
                : $"₹ {Math.Abs(amount):N2} Cr";
        }

        private void BtnBack_Click(object sender, RoutedEventArgs e)
        {
            if (NavigationService?.CanGoBack == true)
                NavigationService.GoBack();
        }

        private async void BtnPrev_Click(object sender, RoutedEventArgs e)
        {
            if (_currentPage > 1)
                await LoadPageAsync(_currentPage - 1);
        }

        private async void BtnNext_Click(object sender, RoutedEventArgs e)
        {
            await LoadPageAsync(_currentPage + 1);
        }
    }
}
