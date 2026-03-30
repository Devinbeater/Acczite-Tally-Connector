using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Acczite20.Data;
using Acczite20.Models;
using Acczite20.Services;
using Acczite20.Services.Explorer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views.Pages
{
    public partial class InventoryExplorerPage : Page, INotifyPropertyChanged
    {
        private readonly AppDbContext _dbContext;
        private readonly Acczite20.Services.Explorer.InventoryExplorerService _inventoryService;

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private ObservableCollection<StockItemDto> _items = new();
        public ObservableCollection<StockItemDto> Items
        {
            get => _items;
            set { _items = value; OnPropertyChanged(); }
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

        public InventoryExplorerPage(AppDbContext dbContext, Acczite20.Services.Explorer.InventoryExplorerService inventoryService)
        {
            InitializeComponent();
            _dbContext = dbContext;
            _inventoryService = inventoryService;
            DataContext = this;
            Loaded += InventoryExplorerPage_Loaded;
        }

        private async void InventoryExplorerPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }

        private async Task LoadDataAsync()
        {
            LoadingRing.IsActive = true;
            LoadingRing.Visibility = Visibility.Visible;
            InventoryGrid.Visibility = Visibility.Collapsed;

            try
            {
                int skip = (CurrentPage - 1) * PageSize;
                var (items, total) = await _inventoryService.SearchStockItemsAsync(SearchBox.Text, skip, PageSize);

                TotalRecords = total;
                TotalPages = (int)Math.Ceiling((double)total / PageSize);
                if (TotalPages == 0) TotalPages = 1;

                Items.Clear();
                foreach (var item in items)
                {
                    Items.Add(item);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load inventory: {ex.Message}", "Inventory Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                LoadingRing.IsActive = false;
                LoadingRing.Visibility = Visibility.Collapsed;
                InventoryGrid.Visibility = Visibility.Visible;
            }
        }

        private async void SearchBox_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                CurrentPage = 1;
                await LoadDataAsync();
            }
        }

        private async void PrevPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage > 1)
            {
                CurrentPage--;
                await LoadDataAsync();
            }
        }

        private async void NextPage_Click(object sender, RoutedEventArgs e)
        {
            if (CurrentPage < TotalPages)
            {
                CurrentPage++;
                await LoadDataAsync();
            }
        }

        private async void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            CurrentPage = 1;
            await LoadDataAsync();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            CurrentPage = 1;
            await LoadDataAsync();
        }

    }
}
