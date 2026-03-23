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
            // Handle Drill-down navigation state
            if (NavigationService != null && NavigationService.Content == this)
            {
               // This is tricky in WPF without OnNavigatedTo. 
               // However, my NavigationService.NavigateTo passes state.
               // ModernWPF Frame.Navigate(page, state) stores it in Frame.NavigationService.Content
            }
            
            // For now, if state was passed via some global or property, we'd use it.
            // Let's assume most page navigation passes thru the DI-resolved instance.
            await LoadLedgersAsync();
        }

        private async System.Threading.Tasks.Task LoadLedgersAsync()
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty && string.IsNullOrEmpty(SessionManager.Instance.OrganizationObjectId)) return;

            var results = await _explorerService.SearchLedgersAsync(
                orgId,
                SearchBox.Text
            );

            Ledgers.Clear();
            foreach (var item in results)
            {
                Ledgers.Add(item);
            }
        }

        private async void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DetailPanel.Visibility = Visibility.Hidden;
            SelectedLedger = null;
            await LoadLedgersAsync();
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
