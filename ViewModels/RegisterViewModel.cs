using System;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using Acczite20.Commands;
using Acczite20.Views.Pages;  // Adjust this to your actual LoginPage namespace

namespace Acczite20.ViewModels
{
    public class RegisterViewModel : INotifyPropertyChanged
    {
        private string _email = string.Empty;
        private string _username = string.Empty;
        private string _password = string.Empty;
        private string _confirmPassword = string.Empty;
        private bool _isBusy;

        public string Email
        {
            get => _email;
            set
            {
                if (_email != value)
                {
                    _email = value;
                    OnPropertyChanged(nameof(Email));
                    UpdateCommandState();
                }
            }
        }

        public string Username
        {
            get => _username;
            set
            {
                if (_username != value)
                {
                    _username = value;
                    OnPropertyChanged(nameof(Username));
                    UpdateCommandState();
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
                    UpdateCommandState();
                }
            }
        }

        public string ConfirmPassword
        {
            get => _confirmPassword;
            set
            {
                if (_confirmPassword != value)
                {
                    _confirmPassword = value;
                    OnPropertyChanged(nameof(ConfirmPassword));
                    UpdateCommandState();
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
                    UpdateCommandState();
                }
            }
        }

        public ICommand RegisterCommand { get; }

        public RegisterViewModel()
        {
            RegisterCommand = new RelayCommand(async _ => await RegisterAsync(), _ => CanRegister());
        }

        private bool CanRegister()
        {
            return !IsBusy
                && !string.IsNullOrWhiteSpace(Email)
                && !string.IsNullOrWhiteSpace(Username)
                && !string.IsNullOrWhiteSpace(Password)
                && !string.IsNullOrWhiteSpace(ConfirmPassword)
                && Password == ConfirmPassword;
        }

        private void UpdateCommandState()
        {
            CommandManager.InvalidateRequerySuggested();
        }

        private async Task RegisterAsync()
        {
            if (IsBusy) return;

            try
            {
                IsBusy = true;

                // TODO: Implement your real registration web API call here.
                await Task.Delay(1000); // Simulating async operation

                MessageBox.Show("Registration successful! Please login.", "Success", MessageBoxButton.OK, MessageBoxImage.Information);

                Application.Current.Dispatcher.Invoke(() =>
                {
                    var loginPage = new LoginPage();
                    var mainWindow = Application.Current.MainWindow;

                    if (mainWindow is not null && mainWindow is System.Windows.Navigation.NavigationWindow navWindow)
                    {
                        navWindow.Navigate(loginPage);
                    }
                    else
                    {
                        var window = new Window { Content = loginPage };
                        window.Show();
                    }
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Registration failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                IsBusy = false;
                UpdateCommandState();
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged(string propertyName) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
