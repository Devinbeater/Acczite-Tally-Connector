using System;
using System.Diagnostics;
using System.Windows;
using Acczite20.Services;
using Acczite20.Services.Authentication;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views
{
    public partial class StartupWindow : Window
    {
        public StartupWindow()
        {
            InitializeComponent();
            Loaded += StartupWindow_Loaded;
        }

        private async void StartupWindow_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                var serviceProvider = ((App)Application.Current).ServiceProvider;
                var persistenceService = serviceProvider.GetRequiredService<SessionPersistenceService>();
                var organizationContextService = serviceProvider.GetRequiredService<OrganizationContextService>();
                var sessionData = await persistenceService.LoadSessionAsync();

                if (sessionData != null && !string.IsNullOrEmpty(sessionData.Token))
                {
                    var session = SessionManager.Instance;
                    session.JwtToken = sessionData.Token;
                    session.Username = sessionData.Username;

                    if (Guid.TryParse(sessionData.UserId, out var userId))
                    {
                        session.UserId = userId;
                    }

                    session.UserObjectId = !string.IsNullOrWhiteSpace(sessionData.UserObjectId)
                        ? sessionData.UserObjectId
                        : sessionData.UserId;

                    if (Guid.TryParse(sessionData.OrganizationId, out var organizationId))
                    {
                        session.OrganizationId = organizationId;
                    }

                    session.OrganizationObjectId = !string.IsNullOrWhiteSpace(sessionData.OrganizationObjectId)
                        ? sessionData.OrganizationObjectId
                        : sessionData.OrganizationId;

                    session.OrganizationName = string.IsNullOrWhiteSpace(sessionData.OrganizationName)
                        ? null
                        : sessionData.OrganizationName;
                    session.Authority = sessionData.Authority;
                    session.IsTrialExpired = sessionData.IsTrialExpired;
                    session.IsAuthenticated = true;

                    if (string.IsNullOrWhiteSpace(session.OrganizationObjectId))
                    {
                        session.OrganizationObjectId = organizationContextService.ExtractOrganizationIdFromToken(session.JwtToken) ?? string.Empty;
                    }

                    if (string.IsNullOrWhiteSpace(session.OrganizationName))
                    {
                        session.OrganizationName = organizationContextService.ExtractOrganizationNameFromToken(session.JwtToken, session.OrganizationObjectId);
                    }

                    if (string.IsNullOrWhiteSpace(session.OrganizationName) && !string.IsNullOrWhiteSpace(session.OrganizationObjectId))
                    {
                        session.OrganizationName = await organizationContextService.ResolveOrganizationNameAsync(session.JwtToken, session.OrganizationObjectId);

                        if (!string.IsNullOrWhiteSpace(session.OrganizationName))
                        {
                            await persistenceService.SaveSessionAsync(new UserSessionData
                            {
                                Token = session.JwtToken ?? string.Empty,
                                UserId = session.UserId != Guid.Empty ? session.UserId.ToString() : session.UserObjectId,
                                UserObjectId = session.UserObjectId,
                                Username = session.Username ?? string.Empty,
                                OrganizationId = session.OrganizationId != Guid.Empty ? session.OrganizationId.ToString() : session.OrganizationObjectId,
                                OrganizationObjectId = session.OrganizationObjectId,
                                OrganizationName = session.OrganizationName,
                                Authority = session.Authority ?? string.Empty,
                                IsTrialExpired = session.IsTrialExpired
                            });
                        }
                    }

                    DialogResult = true;
                    Close();
                }
            }
            catch
            {
            }
        }

        private async void Login_Click(object sender, RoutedEventArgs e)
        {
            string loginId = LoginIdBox.Text;
            string password = PasswordBox.Password;

            if (string.IsNullOrWhiteSpace(loginId) || string.IsNullOrWhiteSpace(password))
            {
                await CustomDialog.ShowAsync("Input Required", "Please enter both Email/Phone and Password.", CustomDialog.DialogType.Warning);
                return;
            }

            try
            {
                LoginBtn.IsEnabled = false;
                LoginBtn.Content = "AUTHENTICATING...";

                var serviceProvider = ((App)Application.Current).ServiceProvider;
                var authService = serviceProvider.GetRequiredService<IAuthenticationService>();

                bool success = await authService.LoginAsync(loginId, password);

                if (success)
                {
                    DialogResult = true;
                    Close();
                }
                else
                {
                    await CustomDialog.ShowAsync("Login Failed", $"Invalid credentials or server error. Please try again.\n\nMake sure the backend is accessible at {serviceProvider.GetRequiredService<Microsoft.Extensions.Configuration.IConfiguration>()["ApiBaseUrl"] ?? "https://api.acczite.in"}.", CustomDialog.DialogType.Error);
                }
            }
            catch (Exception ex)
            {
                var serviceProvider = ((App)Application.Current).ServiceProvider;
                await CustomDialog.ShowAsync("System Error", $"Error during login: {ex.Message}\n\nMake sure the backend is accessible at {serviceProvider.GetRequiredService<Microsoft.Extensions.Configuration.IConfiguration>()["ApiBaseUrl"] ?? "https://api.acczite.in"}.", CustomDialog.DialogType.Error);
            }
            finally
            {
                LoginBtn.IsEnabled = true;
                LoginBtn.Content = "LOGIN TO WORKSPACE";
            }
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        private void Minimize_Click(object sender, RoutedEventArgs e)
        {
            WindowState = WindowState.Minimized;
        }

        private void GetLicense_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                Process.Start(new ProcessStartInfo("https://acczite.in") { UseShellExecute = true });
            }
            catch
            {
            }
        }
        private void Window_MouseDown(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (e.ChangedButton == System.Windows.Input.MouseButton.Left)
                DragMove();
        }
    }
}
