using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Models.Integration;
using Acczite20.Services.Integration;
using Acczite20.Services;

namespace Acczite20.Views.Pages
{
    public partial class MappingReviewPage : Page
    {
        private readonly IEntityMappingService _mappingService;
        public ObservableCollection<PendingMapping> PendingMappings { get; } = new ObservableCollection<PendingMapping>();

        public MappingReviewPage(IEntityMappingService mappingService)
        {
            InitializeComponent();
            _mappingService = mappingService;
            MappingList.ItemsSource = PendingMappings;
            
            this.Loaded += async (s, e) => await LoadDataAsync();
        }

        private async Task LoadDataAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var list = await _mappingService.GetPendingMappingsAsync(orgId);
                
                PendingMappings.Clear();
                foreach (var item in list)
                {
                    PendingMappings.Add(item);
                }

                EmptyState.Visibility = PendingMappings.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error loading mappings: {ex.Message}", "Sync Error");
            }
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDataAsync();
        }

        private async void Approve_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.Tag is Guid id)
            {
                try
                {
                    await _mappingService.ApproveMappingAsync(id);
                    await LoadDataAsync();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Approval failed: {ex.Message}", "Mapping Error");
                }
            }
        }

        private async void Reject_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.Tag is Guid id)
            {
                try
                {
                    await _mappingService.RejectMappingAsync(id);
                    await LoadDataAsync();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Rejection failed: {ex.Message}", "Mapping Error");
                }
            }
        }

        private void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Implementation for filtering could be added here
        }
    }
}
