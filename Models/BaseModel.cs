using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace Acczite20.ViewModels
{
    public class BaseViewModel : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>
        /// Sets the property and notifies UI only if value has changed.
        /// </summary>
        protected bool SetProperty<T>(ref T backingField, T value, [CallerMemberName] string propertyName = "")
        {
            if (EqualityComparer<T>.Default.Equals(backingField, value))
                return false;

            backingField = value;
            OnPropertyChanged(propertyName);
            return true;
        }

        /// <summary>
        /// Raises PropertyChanged event.
        /// </summary>
        protected void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
