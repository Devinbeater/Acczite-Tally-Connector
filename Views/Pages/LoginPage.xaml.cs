using System.Windows;
using System.Windows.Controls;
using Acczite20.ViewModels;

namespace Acczite20.Views.Pages
{
    public partial class LoginPage : Page
    {
        public LoginPage()
        {
            InitializeComponent();
        }

        private void EmailTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
        }

        private void PasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (DataContext is LoginViewModel vm)
            {
                vm.Password = PasswordBox.Password;
            }
        }

        private void RegisterButton_Click(object sender, RoutedEventArgs e)
        {
            if (App.NavigationService != null)
            {
                App.NavigationService.NavigateToRegisterPage();
                return;
            }

            NavigationService?.Navigate(new RegisterPage());
        }
    }
}
