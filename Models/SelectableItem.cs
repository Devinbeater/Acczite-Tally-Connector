// Updated 1: SelectableItem.cs
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace Acczite20.Models
{
    public class SelectableItem : INotifyPropertyChanged
    {
        private string? _name;
        public string? Name
        {
            get => _name;
            set => SetProperty(ref _name, value);
        }

        private bool _isSelected;
        public bool IsSelected
        {
            get => _isSelected;
            set
            {
                if (SetProperty(ref _isSelected, value))
                {
                    OnPropertyChanged(nameof(IsSelected));
                    SelectionChanged?.Invoke(this, EventArgs.Empty);
                }
            }
        }

        private int _sequenceNumber;
        public int SequenceNumber
        {
            get => _sequenceNumber;
            set => SetProperty(ref _sequenceNumber, value);
        }

        private string? _matchedField;
        public string? MatchedField
        {
            get => _matchedField;
            set => SetProperty(ref _matchedField, value);
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        public event EventHandler? SelectionChanged;

        protected void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        protected bool SetProperty<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
        {
            if (EqualityComparer<T>.Default.Equals(field, value)) return false;
            field = value;
            OnPropertyChanged(propertyName);
            return true;
        }
    }
}
