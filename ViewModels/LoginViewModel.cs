using System;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using Acczite20.Commands;
using Acczite20.Services.Authentication;
using Acczite20.Services.Navigation;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.ViewModels
{
    public class LoginViewModel : INotifyPropertyChanged
    {
        private string _loginId = string.Empty;
        private string _password = string.Empty;
        private bool _isBusy;

        public string LoginId
        {
            get => _loginId;
            set
            {
                if (_loginId != value)
                {
                    _loginId = value;
                    OnPropertyChanged(nameof(LoginId));
                }
            }
        }

        public string Password
        {
            get => _password;
            set
            {
                if (_password != value)
                {
                    _password = value;
                    OnPropertyChanged(nameof(Password));
                }
            }
        }

        public bool IsBusy
        {
            get => _isBusy;
            set
            {
                if (_isBusy != value)
                {
                    _isBusy = value;
                    OnPropertyChanged(nameof(IsBusy));
                }
            }
        }

        public ICommand LoginCommand { get; }

        public LoginViewModel()
        {
            LoginCommand = new AsyncRelayCommand(_ => LoginAsync(), _ => !IsBusy);
        }

        private async Task LoginAsync()
        {
            if (IsBusy) return;

            if (string.IsNullOrWhiteSpace(LoginId) || string.IsNullOrWhiteSpace(Password))
            {
                MessageBox.Show("Please enter both Email/Phone and Password.", "Login Required", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                IsBusy = true;

                var serviceProvider = ((App)Application.Current).ServiceProvider;
                var authService = serviceProvider.GetRequiredService<IAuthenticationService>();

                bool success = await authService.LoginAsync(LoginId, Password);

                if (success)
                {
                    Application.Current.Dispatcher.Invoke(async () =>
                    {
                        if (Application.Current.MainWindow is Acczite20.Views.MainWindow mainWin)
                        {
                            await mainWin.RestoreSessionContext();
                        }
                        else 
                        {
                            var navService = serviceProvider.GetRequiredService<INavigationService>();
                            navService.NavigateTo<Acczite20.Views.Pages.DashboardPage>();
                        }
                    });
                }
                else
                {
                    MessageBox.Show("Invalid credentials or error connecting to server.", "Login Failed", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error during login: {ex.Message}\n\nMake sure your backend is accessible.", "Login Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                IsBusy = false;
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged(string propertyName) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
