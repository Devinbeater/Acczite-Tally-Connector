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

        private int _currentPage = 1;
        public int CurrentPage
        {
            get => _currentPage;
            set { _currentPage = value; OnPropertyChanged(); OnPropertyChanged(nameof(CanGoBack)); OnPropertyChanged(nameof(CanGoForward)); }
        }

        private int _totalPages = 1;
        public int TotalPages
        {
            get => _totalPages;
            set { _totalPages = value; OnPropertyChanged(); OnPropertyChanged(nameof(CanGoForward)); }
        }

        private long _totalRecords = 0;
        public long TotalRecords
        {
            get => _totalRecords;
            set { _totalRecords = value; OnPropertyChanged(); }
        }

        private int _pageSize = 50;
        public int PageSize
        {
            get => _pageSize;
            set { _pageSize = value; OnPropertyChanged(); }
        }

        public bool CanGoBack => CurrentPage > 1;
        public bool CanGoForward => CurrentPage < TotalPages;

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

            int skip = (CurrentPage - 1) * PageSize;
            var (items, total) = await _explorerService.SearchVouchersAsync(
                orgId,
                SearchBox.Text,
                FromDatePicker.SelectedDate,
                ToDatePicker.SelectedDate,
                skip,
                PageSize
            );

            TotalRecords = total;
            TotalPages = (int)Math.Ceiling((double)total / PageSize);
            if (TotalPages == 0) TotalPages = 1;

            Vouchers.Clear();
            foreach (var item in items)
            {
                Vouchers.Add(item);
            }
        }

        private async void Search_Click(object sender, RoutedEventArgs e)
        {
            DetailPanel.Visibility = Visibility.Hidden;
            SelectedVoucher = null;
            CurrentPage = 1;
            await LoadVouchersAsync();
        }

        private async void PrevPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage > 1)
            {
                CurrentPage--;
                await LoadVouchersAsync();
            }
        }

        private async void NextPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage < TotalPages)
            {
                CurrentPage++;
                await LoadVouchersAsync();
            }
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
