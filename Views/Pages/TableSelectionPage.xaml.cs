using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Acczite20.ViewModels.Wizard;
using System.Collections.Generic;
using Acczite20.Views.Pages;
using Acczite20.Models;

using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public partial class TableSelectionPage : Page
    {
        private readonly TableSelectionViewModel _viewModel;
        private readonly INavigationService _navigationService;

        public TableSelectionPage(string connectionString, INavigationService navigationService)
        {
            _navigationService = navigationService;
            InitializeComponent();
            _viewModel = new TableSelectionViewModel(connectionString);
            DataContext = _viewModel;

            Loaded += TableSelectionPage_Loaded;
        }

        private void TableSelectionPage_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                _viewModel.LoadTables();

                foreach (var item in _viewModel.AvailableTables)
                {
                    item.SelectionChanged += OnItemSelectionChanged;
                }
            }
            catch (System.Exception ex)
            {
                MessageBox.Show($"❌ Failed to load tables.\n\nDetails: {ex.Message}",
                                "Error",
                                MessageBoxButton.OK,
                                MessageBoxImage.Error);
            }
        }

        private void OnItemSelectionChanged(object? sender, System.EventArgs e)
        {
            int order = 1;
            foreach (var item in _viewModel.AvailableTables.Where(x => x.IsSelected))
            {
                item.SequenceNumber = order++;
            }

            foreach (var item in _viewModel.AvailableTables.Where(x => !x.IsSelected))
            {
                item.SequenceNumber = 0;
            }
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            List<string> selectedTables = _viewModel.GetSelectedTableNames();

            if (selectedTables.Any())
            {
                try
                {
                    _navigationService.NavigateTo(new TallySyncPage(selectedTables, _navigationService));
                }
                catch (System.Exception ex)
                {
                    MessageBox.Show($"❌ Unable to proceed to sync.\n\nDetails: {ex.Message}",
                                    "Navigation Error",
                                    MessageBoxButton.OK,
                                    MessageBoxImage.Error);
                }
            }
            else
            {
                MessageBox.Show("⚠ Please select at least one type to proceed.",
                                "No Selection",
                                MessageBoxButton.OK,
                                MessageBoxImage.Warning);
            }
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack)
            {
                _navigationService.GoBack();
            }
            else
            {
                MessageBox.Show("No previous page available to go back to.",
                                "Back Navigation",
                                MessageBoxButton.OK,
                                MessageBoxImage.Information);
            }
        }
    }
}
