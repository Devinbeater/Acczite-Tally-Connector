using Acczite20.Models;
using Acczite20.Services;
using Acczite20.ViewModels.Base;
using System;
using System.Threading.Tasks;
using System.Windows.Input;
using Acczite20.Commands;

namespace Acczite20.ViewModels
{
    public class ProfileViewModel : BaseViewModel
    {
        private string _username = AppState.LoggedInUser ?? string.Empty;
        public string Username
        {
            get => _username;
            set => SetProperty(ref _username, value);
        }

        private string _email = "user@example.com";
        public string Email
        {
            get => _email;
            set => SetProperty(ref _email, value);
        }

        private string _firstName = "John";
        public string FirstName
        {
            get => _firstName;
            set => SetProperty(ref _firstName, value);
        }

        private string _lastName = "Doe";
        public string LastName
        {
            get => _lastName;
            set => SetProperty(ref _lastName, value);
        }

        public ICommand SaveProfileCommand { get; }

        public int DaysRemaining => (AppState.LicenseExpiryDate - DateTime.Now).Days;

        public ProfileViewModel()
        {
            SaveProfileCommand = new RelayCommand(async _ => await SaveProfileAsync());
        }

        private async Task SaveProfileAsync()
        {
            if (string.IsNullOrWhiteSpace(Username) || string.IsNullOrWhiteSpace(Email))
            {
                return;
            }

            try
            {
                await Task.Delay(200); // Simulate save
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving profile: {ex.Message}");
            }
        }
    }
}
