using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Acczite20.Data;
using Acczite20.Models;
using Acczite20.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views.Pages
{
    public partial class InventoryExplorerPage : Page
    {
        private readonly AppDbContext _dbContext;
        private List<StockItem> _allItems = new();

        public InventoryExplorerPage(AppDbContext dbContext)
        {
            InitializeComponent();
            _dbContext = dbContext;
            Loaded += InventoryExplorerPage_Loaded;
        }

        private async void InventoryExplorerPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }

        private async Task LoadDataAsync()
        {
            LoadingRing.Visibility = Visibility.Visible;
            InventoryGrid.Visibility = Visibility.Collapsed;

            try
            {
                var dbType = SessionManager.Instance.SelectedDatabaseType;
                var orgId = SessionManager.Instance.OrganizationId;
                var orgObjectId = SessionManager.Instance.OrganizationObjectId;
                
                (Application.Current as App)?.LogBreadcrumb($"InventoryExplorerPage: Loading data. DB={dbType}, SQLOrg={orgId}, MongoOrg={orgObjectId}");

                if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
                {
                    var mongoService = ((App)Application.Current).ServiceProvider.GetRequiredService<Acczite20.Services.MongoService>();
                    var allCollections = await mongoService.ListCollectionsAsync();
                    (Application.Current as App)?.LogBreadcrumb($"InventoryExplorerPage: Mongo Collections: {string.Join(", ", allCollections)}");

                    var inventoryService = ((App)Application.Current).ServiceProvider.GetRequiredService<Acczite20.Services.Explorer.InventoryExplorerService>();
                    var items = await inventoryService.SearchStockItemsAsync(SearchBox.Text);
                    
                    (Application.Current as App)?.LogBreadcrumb($"InventoryExplorerPage: Found {items.Count} items in MongoDB");

                    _allItems = items.Select(i => new StockItem 
                    { 
                        Name = i.Name, 
                        StockGroup = i.StockGroup,
                        BaseUnit = i.Unit,
                        OpeningBalance = 0, // MongoDB might not have this easily
                        ClosingBalance = i.ClosingBalance
                    }).ToList();
                }
                else
                {
                    (Application.Current as App)?.LogBreadcrumb($"InventoryExplorerPage: Querying SQL for org {orgId}");
                    _allItems = await _dbContext.StockItems
                        .Where(s => s.OrganizationId == orgId)
                        .OrderBy(s => s.Name)
                        .ToListAsync();
                    (Application.Current as App)?.LogBreadcrumb($"InventoryExplorerPage: Found {_allItems.Count} items in SQL");
                }

                // Prepare groups for filtering
                var groups = _allItems.Select(s => s.StockGroup).Distinct().OrderBy(g => g).ToList();
                groups.Insert(0, "All Groups");
                GroupFilter.ItemsSource = groups;
                GroupFilter.SelectedIndex = 0;

                UpdateFilteredGrid();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load inventory: {ex.Message}", "Inventory Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                LoadingRing.Visibility = Visibility.Collapsed;
                InventoryGrid.Visibility = Visibility.Visible;
            }
        }

        private void UpdateFilteredGrid()
        {
            var searchText = SearchBox.Text.ToLower().Trim();
            var selectedGroup = GroupFilter.SelectedItem as string;

            var filtered = _allItems.Where(s => 
                (string.IsNullOrEmpty(searchText) || s.Name.ToLower().Contains(searchText)) &&
                (selectedGroup == "All Groups" || s.StockGroup == selectedGroup)
            ).ToList();

            InventoryGrid.ItemsSource = filtered;
        }

        private void SearchBox_KeyUp(object sender, KeyEventArgs e)
        {
            UpdateFilteredGrid();
        }

        private void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            UpdateFilteredGrid();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }
    }
}
