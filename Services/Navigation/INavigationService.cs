using System.Windows.Controls;

namespace Acczite20.Services.Navigation
{
    public interface INavigationService
    {
        void NavigateTo(Page view);

        void NavigateTo<T>(object? navigationState = null) where T : Page;

        void NavigateToLoginPage(object? navigationState = null);

        void NavigateToRegisterPage(object? navigationState = null);

        void NavigateToMainPage(object? navigationState = null);
        void GoBack();
        bool CanGoBack { get; }
    }
}
