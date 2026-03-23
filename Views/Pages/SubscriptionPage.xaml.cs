using System.Windows;
using System.Windows.Controls;
using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public partial class SubscriptionPage : Page
    {
        private readonly INavigationService _navigationService;

        public SubscriptionPage(INavigationService navigationService)
        {
            _navigationService = navigationService;
            InitializeComponent();
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack) _navigationService.GoBack();
        }

        private void SelectStarter_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Starter plan selected.\nYou will be redirected to payment.", "Plan Selection", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void SelectPro_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Professional plan selected.\nYou will be redirected to payment.", "Plan Selection", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void SelectEnterprise_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Our team will contact you within 24 hours.\nEmail: sales@acczite.com", "Enterprise Inquiry", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void ActivateLicense_Click(object sender, RoutedEventArgs e)
        {
            if (string.IsNullOrWhiteSpace(LicenseKeyBox.Text))
            {
                MessageBox.Show("Please enter a valid license key.", "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // TODO: Validate license key against API
            MessageBox.Show($"License key submitted for validation.\nKey: {LicenseKeyBox.Text}", "Activation", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }
}
