using System;
using System.Threading.Tasks;
using System.Windows.Input;
using Acczite20.Commands;
using Acczite20.Services.Navigation;
using Acczite20.ViewModels.Base;

namespace Acczite20.ViewModels
{
    public class MainViewModel : BaseViewModel
    {
        private readonly INavigationService _navigationService;

        public ICommand LogoutCommand { get; }

        public MainViewModel(INavigationService navigationService)
        {
            _navigationService = navigationService ?? throw new ArgumentNullException(nameof(navigationService));
            LogoutCommand = new RelayCommand(async _ => await OnLogoutAsync());
        }

        private async Task OnLogoutAsync()
        {
            await Task.Delay(100); // Simulated async
            _navigationService.NavigateToLoginPage();
        }
    }
}
