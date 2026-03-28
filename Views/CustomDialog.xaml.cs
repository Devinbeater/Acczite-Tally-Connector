using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;

namespace Acczite20.Views
{
    public partial class CustomDialog : Window
    {
        public enum DialogType { Success, Error, Warning, Info }
        private TaskCompletionSource<bool>? _tcs;

        public CustomDialog()
        {
            InitializeComponent();
        }

        public static Task<bool> ShowAsync(string title, string message, DialogType type = DialogType.Info, string primaryText = "OK", string secondaryText = "")
        {
            var dialog = new CustomDialog();
            dialog.TxtTitle.Text = title;
            dialog.TxtMessage.Text = message;
            dialog.BtnPrimary.Content = primaryText;
            
            if (!string.IsNullOrEmpty(secondaryText))
            {
                dialog.BtnSecondary.Content = secondaryText;
                dialog.BtnSecondary.Visibility = Visibility.Visible;
            }

            // Apply theme based on type
            switch (type)
            {
                case DialogType.Success:
                    dialog.BorderIcon.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#DCFCE7"));
                    dialog.TxtIcon.Text = "\uE73E"; // Accept/Check
                    dialog.TxtIcon.Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#166534"));
                    break;
                case DialogType.Error:
                    dialog.BorderIcon.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#FEE2E2"));
                    dialog.TxtIcon.Text = "\uE711"; // Cancel/Error
                    dialog.TxtIcon.Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#991B1B"));
                    break;
                case DialogType.Warning:
                    dialog.BorderIcon.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#FEF3C7"));
                    dialog.TxtIcon.Text = "\uE7BA"; // Warning
                    dialog.TxtIcon.Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#92400E"));
                    break;
                case DialogType.Info:
                default:
                    dialog.BorderIcon.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#DBEAFE"));
                    dialog.TxtIcon.Text = "\uE946"; // Info (different glyph for info circle)
                    dialog.TxtIcon.Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#1E40AF"));
                    break;
            }

            dialog.Owner = Application.Current.MainWindow;
            dialog._tcs = new TaskCompletionSource<bool>();
            dialog.Show();
            return dialog._tcs.Task;
        }

        private void Primary_Click(object sender, RoutedEventArgs e)
        {
            _tcs?.TrySetResult(true);
            Close();
        }

        private void Secondary_Click(object sender, RoutedEventArgs e)
        {
            _tcs?.TrySetResult(false);
            Close();
        }
    }
}
