using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.MongoDocuments;
using Acczite20.Services;

namespace Acczite20.Views.Pages
{
    public partial class AdminInventoryPage : Page, System.ComponentModel.INotifyPropertyChanged
    {
        private readonly AdminService _adminService;
        
        public ObservableCollection<AdminInventoryDocument> InventoryRecords { get; set; } = new ObservableCollection<AdminInventoryDocument>();
        public ObservableCollection<AdminProductDocument> ProductRecords { get; set; } = new ObservableCollection<AdminProductDocument>();

        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName) => PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));

        public AdminInventoryPage(AdminService adminService)
        {
            _adminService = adminService;
            InitializeComponent();
            DataContext = this;
            Loaded += AdminInventoryPage_Loaded;
        }

        private async void AdminInventoryPage_Loaded(object sender, RoutedEventArgs e)
        {
            await RefreshAll();
        }

        private async Task RefreshAll()
        {
            try
            {
                LoadingRing.Visibility = Visibility.Visible;
                
                var inventory = await _adminService.GetInventoryAsync();
                InventoryRecords.Clear();
                foreach (var item in inventory) InventoryRecords.Add(item);

                var products = await _adminService.GetProductsAsync();
                ProductRecords.Clear();
                foreach (var item in products) ProductRecords.Add(item);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load Admin Inventory data: {ex.Message}", "Error");
            }
            finally
            {
                LoadingRing.Visibility = Visibility.Collapsed;
            }
        }

        private async void RefreshInventory_Click(object sender, RoutedEventArgs e) => await RefreshAll();
        private async void RefreshProducts_Click(object sender, RoutedEventArgs e) => await RefreshAll();
    }
}
