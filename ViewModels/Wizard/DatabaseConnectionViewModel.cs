using Acczite20.ViewModels.Base;
using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows.Input;
using Acczite20.Services;
using Microsoft.Data.SqlClient;
using Acczite20.Commands;

namespace Acczite20.ViewModels.Wizard
{
    public class DatabaseConnectionViewModel : BaseViewModel
    {
        private string _serverName = string.Empty;
        private string _databaseName = string.Empty;
        private string _username = string.Empty;
        private string _password = string.Empty;
        private string _connectionStatus = "Not Connected";

        public string ServerName
        {
            get => _serverName;
            set => SetProperty(ref _serverName, value);
        }

        public string DatabaseName
        {
            get => _databaseName;
            set => SetProperty(ref _databaseName, value);
        }

        public string Username
        {
            get => _username;
            set => SetProperty(ref _username, value);
        }

        public string Password
        {
            get => _password;
            set => SetProperty(ref _password, value);
        }

        public string ConnectionStatus
        {
            get => _connectionStatus;
            set => SetProperty(ref _connectionStatus, value);
        }

        public ICommand TestConnectionCommand { get; }

        public DatabaseConnectionViewModel()
        {
            TestConnectionCommand = new RelayCommand(async _ => await TestConnectionAsync(), _ => CanTestConnection());
        }

        private bool CanTestConnection()
        {
            return !string.IsNullOrWhiteSpace(ServerName) &&
                   !string.IsNullOrWhiteSpace(DatabaseName);
        }

        private async Task TestConnectionAsync()
        {
            try
            {
                var connectionString = $"Server={ServerName};Database={DatabaseName};User Id={Username};Password={Password};";
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    ConnectionStatus = "Connected";
                }
            }
            catch (Exception ex)
            {
                ConnectionStatus = $"Connection failed: {ex.Message}";
            }
        }
    }
}