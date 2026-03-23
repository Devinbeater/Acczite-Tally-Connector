using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services.Analytics;
using Acczite20.Services;

namespace Acczite20.Views.Pages
{
    public partial class GlobalSearchPage : Page
    {
        private readonly IGlobalSearchService _searchService;

        public GlobalSearchPage(IGlobalSearchService searchService)
        {
            InitializeComponent();
            _searchService = searchService;
        }

        private async void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            var query = SearchBox.Text;
            if (string.IsNullOrWhiteSpace(query) || query.Length < 3)
            {
                ResultsList.ItemsSource = null;
                return;
            }

            SearchProgress.IsActive = true;
            SearchProgress.Visibility = Visibility.Visible;

            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var results = await _searchService.SearchAsync(orgId, query);
                ResultsList.ItemsSource = results;
            }
            catch
            {
                ResultsList.ItemsSource = null;
            }
            finally
            {
                SearchProgress.IsActive = false;
                SearchProgress.Visibility = Visibility.Collapsed;
            }
        }

        private void ResultsList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            var selectedResult = ResultsList.SelectedItem as GlobalSearchResult;
            if (selectedResult != null)
            {
                MessageBox.Show($"Selected: {selectedResult.Title}\nAction: {selectedResult.DetailActionId}", "Search Action Triggered");
                ResultsList.SelectedIndex = -1; // Reset
            }
        }
    }
}
