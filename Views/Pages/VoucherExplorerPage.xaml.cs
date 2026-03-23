using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Explorer;

namespace Acczite20.Views.Pages
{
    public partial class VoucherExplorerPage : Page, INotifyPropertyChanged
    {
        private readonly VoucherExplorerService _explorerService;

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private ObservableCollection<VoucherListItem> _vouchers = new();
        public ObservableCollection<VoucherListItem> Vouchers
        {
            get => _vouchers;
            set { _vouchers = value; OnPropertyChanged(); }
        }

        private VoucherDetailDto? _selectedVoucher;
        public VoucherDetailDto? SelectedVoucher
        {
            get => _selectedVoucher;
            set { _selectedVoucher = value; OnPropertyChanged(); }
        }

        public VoucherExplorerPage(VoucherExplorerService explorerService)
        {
            _explorerService = explorerService;
            InitializeComponent();
            DataContext = this;
            Loaded += VoucherExplorerPage_Loaded;
        }

        private async void VoucherExplorerPage_Loaded(object sender, RoutedEventArgs e)
        {
            ToDatePicker.SelectedDate = DateTime.Today;
            FromDatePicker.SelectedDate = DateTime.Today.AddDays(-30);

            await LoadVouchersAsync();
        }

        private async System.Threading.Tasks.Task LoadVouchersAsync()
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty && string.IsNullOrEmpty(SessionManager.Instance.OrganizationObjectId)) return;

            var results = await _explorerService.SearchVouchersAsync(
                orgId,
                SearchBox.Text,
                FromDatePicker.SelectedDate,
                ToDatePicker.SelectedDate
            );

            Vouchers.Clear();
            foreach (var item in results)
            {
                Vouchers.Add(item);
            }
        }

        private async void Search_Click(object sender, RoutedEventArgs e)
        {
            DetailPanel.Visibility = Visibility.Hidden;
            SelectedVoucher = null;
            await LoadVouchersAsync();
        }

        private async void VouchersGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (VouchersGrid.SelectedItem is VoucherListItem selectedItem)
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var details = await _explorerService.GetVoucherAsync(selectedItem.RawId, orgId);
                
                if (details != null)
                {
                    SelectedVoucher = details;
                    DetailPanel.Visibility = Visibility.Visible;
                }
            }
        }
    }
}
