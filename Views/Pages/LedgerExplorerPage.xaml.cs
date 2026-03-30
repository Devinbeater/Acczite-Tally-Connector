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
    public partial class LedgerExplorerPage : Page, INotifyPropertyChanged
    {
        private readonly LedgerExplorerService _explorerService;

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private ObservableCollection<LedgerListItem> _ledgers = new();
        public ObservableCollection<LedgerListItem> Ledgers
        {
            get => _ledgers;
            set { _ledgers = value; OnPropertyChanged(); }
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

        private LedgerDetailDto? _selectedLedger;
        public LedgerDetailDto? SelectedLedger
        {
            get => _selectedLedger;
            set { _selectedLedger = value; OnPropertyChanged(); }
        }

        public LedgerExplorerPage(LedgerExplorerService explorerService)
        {
            _explorerService = explorerService;
            InitializeComponent();
            DataContext = this;
            Loaded += LedgerExplorerPage_Loaded;
        }

        private async void LedgerExplorerPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadLedgersAsync();
        }

        private async System.Threading.Tasks.Task LoadLedgersAsync()
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty && string.IsNullOrEmpty(SessionManager.Instance.OrganizationObjectId)) return;

            int skip = (CurrentPage - 1) * PageSize;
            var (items, total) = await _explorerService.SearchLedgersAsync(
                orgId,
                SearchBox.Text,
                skip,
                PageSize
            );

            TotalRecords = total;
            TotalPages = (int)Math.Ceiling((double)total / PageSize);
            if (TotalPages == 0) TotalPages = 1;

            Ledgers.Clear();
            foreach (var item in items)
            {
                Ledgers.Add(item);
            }
        }

        private async void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DetailPanel.Visibility = Visibility.Hidden;
            SelectedLedger = null;
            CurrentPage = 1;
            await LoadLedgersAsync();
        }

        private async void PrevPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage > 1)
            {
                CurrentPage--;
                await LoadLedgersAsync();
            }
        }

        private async void NextPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage < TotalPages)
            {
                CurrentPage++;
                await LoadLedgersAsync();
            }
        }

        private async void LedgersGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (LedgersGrid.SelectedItem is LedgerListItem selectedItem)
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var details = await _explorerService.GetLedgerDetailsAsync(selectedItem.RawId, orgId);
                
                if (details != null)
                {
                    SelectedLedger = details;
                    DetailPanel.Visibility = Visibility.Visible;
                }
            }
        }
    }
}
