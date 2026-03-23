using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Models.History;
using Acczite20.Services.History;

namespace Acczite20.Views.Pages
{
    public partial class TimelinePage : Page
    {
        private readonly ITimelineService _timelineService;
        public ObservableCollection<UnifiedActivityLog> Activities { get; } = new ObservableCollection<UnifiedActivityLog>();

        public TimelinePage(ITimelineService timelineService)
        {
            InitializeComponent();
            _timelineService = timelineService;
            TimelineList.ItemsSource = Activities;

            this.Loaded += async (s, e) => await LoadTimelineAsync();
        }

        private async Task LoadTimelineAsync()
        {
            try
            {
                var list = await _timelineService.GetRecentActivitiesAsync(100);
                Activities.Clear();
                foreach (var item in list)
                {
                    Activities.Add(item);
                }

                EmptyState.Visibility = Activities.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Could not load timeline: {ex.Message}", "Timeline Error");
            }
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadTimelineAsync();
        }
    }
}
