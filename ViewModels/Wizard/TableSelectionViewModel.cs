using Acczite20.Models;
using Acczite20.ViewModels.Base;
using MySqlConnector;
using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Collections.Generic;
using System.Windows;

namespace Acczite20.ViewModels.Wizard
{
    public class TableSelectionViewModel : BaseViewModel
    {
        private readonly string _connectionString;

        public ObservableCollection<SelectableItem> AvailableTables { get; set; } = new();

        public TableSelectionViewModel(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void LoadTables()
        {
            AvailableTables.Clear();

            try
            {
                using MySqlConnection connection = new(_connectionString);
                connection.Open();

                string query = "SHOW TABLES";
                using MySqlCommand command = new(query, connection);
                using MySqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    string? tableName = reader.GetString(0);
                    if (!string.IsNullOrWhiteSpace(tableName))
                    {
                        var item = new SelectableItem
                        {
                            Name = tableName,
                            IsSelected = false,
                            SequenceNumber = 0,
                            MatchedField = string.Empty
                        };

                        item.SelectionChanged += HandleSelectionChanged;
                        AvailableTables.Add(item);
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error loading MySQL tables: " + ex.Message);
            }
        }

        private void HandleSelectionChanged(object? sender, EventArgs e)
        {
            // Recalculate the order of selected items
            int order = 1;

            foreach (var item in AvailableTables.Where(x => x.IsSelected))
            {
                item.SequenceNumber = order++;
            }

            foreach (var item in AvailableTables.Where(x => !x.IsSelected))
            {
                item.SequenceNumber = 0;
            }
        }

        public List<string> GetSelectedTableNames()
        {
            return AvailableTables
                .Where(t => t.IsSelected)
                .OrderBy(t => t.SequenceNumber)
                .Select(t => t.Name!)
                .ToList();
        }
    }
}
