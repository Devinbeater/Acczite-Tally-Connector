using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Explorer;

namespace Acczite20.Views.Pages
{
    public partial class DaybookPage : Page
    {
        private readonly VoucherExplorerService _voucherService;
        private readonly SessionManager _sessionManager;

        public ObservableCollection<DaybookItem> DaybookItems { get; } = new();

        public DaybookPage(VoucherExplorerService voucherService, SessionManager sessionManager)
        {
            _voucherService = voucherService;
            _sessionManager = sessionManager;
            InitializeComponent();
            DaybookGrid.ItemsSource = DaybookItems;

            // Set default date range to current month
            var now = DateTime.Now;
            FromDatePicker.SelectedDate = new DateTime(now.Year, now.Month, 1);
            ToDatePicker.SelectedDate = now;
        }

        private async void Page_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }

        private async Task LoadDataAsync()
        {
            try
            {
                LoadingRing.IsActive = true;
                LoadingRing.Visibility = Visibility.Visible;
                DaybookGrid.Visibility = Visibility.Collapsed;
                EmptyStatePanel.Visibility = Visibility.Collapsed;

                var from = FromDatePicker.SelectedDate;
                var to = ToDatePicker.SelectedDate;

                var vouchers = await _voucherService.SearchVouchersAsync(
                    _sessionManager.OrganizationId,
                    search: string.Empty,
                    from: from,
                    to: to);

                DaybookItems.Clear();
                foreach (var v in vouchers.OrderBy(x => x.Date))
                {
                    DaybookItems.Add(new DaybookItem
                    {
                        VoucherDate = v.Date.DateTime,
                        VoucherNumber = v.VoucherNumber,
                        VoucherType = new VoucherTypeStub { Name = v.VoucherTypeName },
                        MainLedgerName = v.PartyLedgerName,
                        Narration = v.Narration,
                        TotalAmount = v.Amount
                    });
                }

                if (DaybookItems.Count == 0)
                {
                    EmptyStatePanel.Visibility = Visibility.Visible;
                }
                else
                {
                    DaybookGrid.Visibility = Visibility.Visible;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load daybook data: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                LoadingRing.IsActive = false;
                LoadingRing.Visibility = Visibility.Collapsed;
            }
        }
    }

    public class DaybookItem
    {
        public DateTime VoucherDate { get; set; }
        public string VoucherNumber { get; set; } = string.Empty;
        public VoucherTypeStub VoucherType { get; set; } = new();
        public string MainLedgerName { get; set; } = string.Empty;
        public string Narration { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
    }

    public class VoucherTypeStub
    {
        public string Name { get; set; } = string.Empty;
    }
}
