using System;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Views;
using Acczite20.Views.Pages;

namespace Acczite20.Services.Navigation
{
    public class NavigationService : INavigationService
    {
        private readonly Frame _mainFrame;
        private readonly IServiceProvider _serviceProvider;
        private static readonly object _lock = new object();

        public NavigationService(Frame mainFrame, IServiceProvider serviceProvider)
        {
            _mainFrame = mainFrame ?? throw new ArgumentNullException(nameof(mainFrame));
            _serviceProvider = serviceProvider;
        }

        public void NavigateTo(Page view)
        {
            lock (_lock)
            {
                Application.Current?.Dispatcher.Invoke(() =>
                {
                    _mainFrame.Navigate(view);
                    UpdateActivePageTitle(view);
                });
            }
        }

        public void NavigateTo<T>(object? navigationState = null) where T : Page
        {
            lock (_lock)
            {
                Application.Current?.Dispatcher.Invoke(() =>
                {
                    var page = (T)Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService(_serviceProvider, typeof(T));
                    _mainFrame.Navigate(page, navigationState);
                    UpdateActivePageTitle(page);
                });
            }
        }

        public void NavigateToLoginPage(object? navigationState = null)
        {
            NavigateTo<LoginPage>(navigationState);
        }

        public void NavigateToRegisterPage(object? navigationState = null)
        {
            NavigateTo<RegisterPage>(navigationState);
        }

        public void NavigateToMainPage(object? navigationState = null)
        {
            NavigateTo<MainPage>(navigationState);
        }

        public void GoBack()
        {
            Application.Current?.Dispatcher.Invoke(() =>
            {
                if (_mainFrame.CanGoBack) _mainFrame.GoBack();
            });
        }

        public bool CanGoBack => _mainFrame.CanGoBack;

        private void UpdateActivePageTitle(Page page)
        {
            if (Application.Current?.MainWindow is not MainWindow mainWindow)
            {
                return;
            }

            mainWindow.SetActivePageTitle(page switch
            {
                LoginPage => "Login",
                RegisterPage => "Register",
                MainPage => "Home",
                DashboardPage => "Dashboard",
                ReportsPage => "Reports",
                LogsPage => "System Logs",
                SettingsPage => "Settings",
                SubscriptionPage => "Subscription",
                DatabaseConnectionPage => "Database Configuration",
                TallySyncPage => "Tally Sync Control",
                TallyExecutePage => "Tally Execute",
                TableSelectionPage => "Table Selection",
                _ => page.Title
            });
        }
    }
}
