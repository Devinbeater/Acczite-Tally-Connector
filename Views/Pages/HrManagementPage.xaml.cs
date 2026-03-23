using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.MongoDocuments;
using Acczite20.Services;
using CommunityToolkit.Mvvm.ComponentModel;

namespace Acczite20.Views.Pages
{
    public partial class HrManagementPage : Page, System.ComponentModel.INotifyPropertyChanged
    {
        private readonly HrService _hrService;
        private int _initialTabIndex;
        
        public ObservableCollection<HrAttendanceDocument> AttendanceRecords { get; set; } = new ObservableCollection<HrAttendanceDocument>();
        public ObservableCollection<HrPayrollDocument> PayrollRecords { get; set; } = new ObservableCollection<HrPayrollDocument>();
        public ObservableCollection<HrEmployeeDocument> Employees { get; set; } = new ObservableCollection<HrEmployeeDocument>();

        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName) => PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));

        public HrManagementPage(HrService hrService)
        {
            _hrService = hrService;
            InitializeComponent();
            DataContext = this;
            Loaded += HrManagementPage_Loaded;
        }

        private async void HrManagementPage_Loaded(object sender, RoutedEventArgs e)
        {
            HrTabs.SelectedIndex = _initialTabIndex;
            await RefreshAll();
        }

        public void SetInitialTab(int tabIndex)
        {
            _initialTabIndex = Math.Max(0, Math.Min(tabIndex, 2));

            if (HrTabs != null)
            {
                HrTabs.SelectedIndex = _initialTabIndex;
            }
        }

        private async Task RefreshAll()
        {
            try
            {
                LoadingRing.Visibility = Visibility.Visible;
                
                var attendance = await _hrService.GetAttendanceAsync();
                AttendanceRecords.Clear();
                foreach (var item in attendance) AttendanceRecords.Add(item);

                var payroll = await _hrService.GetPayrollAsync();
                PayrollRecords.Clear();
                foreach (var item in payroll) PayrollRecords.Add(item);

                var employees = await _hrService.GetEmployeesAsync();
                Employees.Clear();
                foreach (var item in employees) Employees.Add(item);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load HR data: {ex.Message}", "Error");
            }
            finally
            {
                LoadingRing.Visibility = Visibility.Collapsed;
            }
        }

        private async void RefreshAttendance_Click(object sender, RoutedEventArgs e) => await RefreshAll();
        private async void RefreshPayroll_Click(object sender, RoutedEventArgs e) => await RefreshAll();
        private async void RefreshEmployees_Click(object sender, RoutedEventArgs e) => await RefreshAll();
    }
}
