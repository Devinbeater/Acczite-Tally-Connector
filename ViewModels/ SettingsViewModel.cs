using Acczite20.ViewModels.Base;
using Acczite20.Commands;
using System.Windows.Input;
using System.Windows;
using System.Threading.Tasks;

namespace Acczite20.ViewModels
{
    public class SettingsViewModel : BaseViewModel
    {
        private string _selectedTheme = "Light";
        private bool _enableNotifications = true;
        private bool _autoLaunch = false;

        public SettingsViewModel()
        {
            SaveSettingsCommand = new RelayCommand(async _ => await SaveSettingsAsync());
        }

        public string SelectedTheme
        {
            get => _selectedTheme;
            set => SetProperty(ref _selectedTheme, value);
        }

        public bool EnableNotifications
        {
            get => _enableNotifications;
            set => SetProperty(ref _enableNotifications, value);
        }

        public bool AutoLaunch
        {
            get => _autoLaunch;
            set => SetProperty(ref _autoLaunch, value);
        }

        public ICommand SaveSettingsCommand { get; }

        private async Task SaveSettingsAsync()
        {
            // Simulate async work
            await Task.Delay(200);
            MessageBox.Show("✅ Settings saved successfully!", "Saved", MessageBoxButton.OK);
        }
    }
}