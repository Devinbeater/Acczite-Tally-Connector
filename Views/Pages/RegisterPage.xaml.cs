using System.Windows;
using System.Windows.Controls;
using Acczite20.ViewModels;
using Acczite20.Commands;

namespace Acczite20.Views.Pages
{
    public partial class RegisterPage : Page
    {
        public RegisterPage()
        {
            InitializeComponent();
        }

        // Update ViewModel Password when PasswordBox changes
        private void PasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (DataContext is RegisterViewModel vm && sender is PasswordBox pb)
            {
                vm.Password = pb.Password;
            }
        }

        // Update ViewModel ConfirmPassword when ConfirmPasswordBox changes
        private void ConfirmPasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (DataContext is RegisterViewModel vm && sender is PasswordBox pb)
            {
                vm.ConfirmPassword = pb.Password;
            }
        }

        // Register button click triggers registration if valid
        private void RegisterButton_Click(object sender, RoutedEventArgs e)
        {
            if (DataContext is RegisterViewModel vm)
            {
                if (vm.RegisterCommand != null && vm.RegisterCommand.CanExecute(null))
                {
                    try
                    {
                        // Execute handles async internally (async void)
                        vm.RegisterCommand.Execute(null);
                    }
                    catch (System.Exception ex)
                    {
                        MessageBox.Show($"Registration failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
                else
                {
                    MessageBox.Show("Please ensure all fields are valid before registering.", "Input Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                }
            }
            else
            {
                MessageBox.Show("Registration form is not initialized correctly.", "Data Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        // Login button click placeholder for navigation
        private void LoginButton_Click(object sender, RoutedEventArgs e)
        {
            if (App.NavigationService != null)
            {
                App.NavigationService.NavigateToLoginPage();
            }
            else
            {
                NavigationService?.Navigate(new LoginPage());
            }
        }
    }
}
