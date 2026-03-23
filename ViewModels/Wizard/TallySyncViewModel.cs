using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Acczite20.Models;
using Acczite20.Services;

namespace Acczite20.ViewModels.Wizard
{
    public class TallySyncViewModel : INotifyPropertyChanged
    {
        private readonly TallyXmlService _tallyService;

        public TallySyncViewModel(TallyXmlService tallyService)
        {
            _tallyService = tallyService;
        }

        public ObservableCollection<SelectableItem> TallyFields { get; set; } = new();

        private bool _isLoading;
        public bool IsLoading
        {
            get => _isLoading;
            set { _isLoading = value; OnPropertyChanged(); }
        }

        private string _statusMessage = "Checking Tally connection...";
        public string StatusMessage
        {
            get => _statusMessage;
            set { _statusMessage = value; OnPropertyChanged(); }
        }

        private bool _isTallyConnected;
        public bool IsTallyConnected
        {
            get => _isTallyConnected;
            set { _isTallyConnected = value; OnPropertyChanged(); }
        }

        private string _companyName = "None";
        public string CompanyName
        {
            get => _companyName;
            set { _companyName = value; OnPropertyChanged(); }
        }

        public ObservableCollection<string> Companies { get; set; } = new();

        private string? _selectedCompany;
        public string? SelectedCompany
        {
            get => _selectedCompany;
            set 
            { 
                if (_selectedCompany == value) return;
                _selectedCompany = value; 
                OnPropertyChanged();
                if (!string.IsNullOrEmpty(value))
                {
                    CompanyName = value;
                    SessionManager.Instance.TallyCompanyName = value;
                    // Trigger refresh of collections for this company
                    _ = LoadCollectionsFromTallyAsync(true);
                }
            }
        }

        public TallySyncViewModel()
        {
            // No hardcoded data — everything loads dynamically
        }

        /// <summary>
        /// Loads live Tally collections by probing the XML API.
        /// Falls back to offline message if Tally is not running.
        /// </summary>
        public async Task LoadCollectionsFromTallyAsync(bool skipCompanyFetch = false)
        {
            IsLoading = true;
            TallyFields.Clear();
            if (!skipCompanyFetch) Companies.Clear();

            try
            {
                // 1. Check if Tally is running
                bool running = await _tallyService.IsTallyRunningAsync();
                IsTallyConnected = running;

                if (!running)
                {
                    StatusMessage = "Tally is not running. Start Tally ERP and try again.";
                    CompanyName = "None";
                    return;
                }

                if (!skipCompanyFetch)
                {
                    // Fetch all available companies but prioritize the ACTIVE one
                    var companies = await _tallyService.GetTallyCompaniesAsync();
                    var activeCompany = await _tallyService.GetCurrentCompanyNameAsync();

                    foreach (var comp in companies) 
                    {
                        if (!Companies.Contains(comp))
                            Companies.Add(comp);
                    }

                    // Ensure active company is in the list and selected
                    if (activeCompany != "None" && !Companies.Contains(activeCompany))
                    {
                        Companies.Insert(0, activeCompany);
                    }

                    if (Companies.Count > 0)
                    {
                        if (activeCompany != "None")
                        {
                            SelectedCompany = activeCompany;
                        }
                        else
                        {
                            var sessionCompany = SessionManager.Instance.TallyCompanyName;
                            SelectedCompany = (!string.IsNullOrEmpty(sessionCompany) && Companies.Contains(sessionCompany))
                                ? sessionCompany
                                : Companies[0];
                        }
                        
                        CompanyName = SelectedCompany ?? "None";
                        SessionManager.Instance.TallyCompanyName = CompanyName;
                    }
                    else 
                    {
                        CompanyName = "None";
                    }
                }

                StatusMessage = "Fetching collections from Tally...";

                // 2. Fetch available collection names
                var collections = await _tallyService.GetTallyCollectionsAsync();

                if (collections.Count == 0)
                {
                    StatusMessage = "No collections found. Open a company in Tally first.";
                    return;
                }

                // 3. Populate the selectable list
                foreach (var col in collections)
                {
                    TallyFields.Add(new SelectableItem { Name = col });
                }

                StatusMessage = $"Found {collections.Count} available collections from Tally.";
            }
            catch (Exception ex)
            {
                StatusMessage = $"Error connecting to Tally: {ex.Message}";
                IsTallyConnected = false;
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Loads dynamic fields for a specific collection by parsing XML response nodes.
        /// </summary>
        public async Task<List<string>> LoadFieldsForCollectionAsync(string collectionName)
        {
            return await _tallyService.GetCollectionFieldsAsync(collectionName);
        }

        public List<string> GetSelectedFields()
        {
            return TallyFields
                .Where(f => f.IsSelected)
                .OrderBy(f => f.SequenceNumber)
                .Select(f => f.Name ?? string.Empty)
                .ToList();
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string? name = null)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
