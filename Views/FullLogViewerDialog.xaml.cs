using System;
using System.Collections;
using System.Collections.Specialized;
using ModernWpf.Controls;
using Acczite20.Services.Sync;
using System.Windows.Controls;

namespace Acczite20.Views
{
    public partial class FullLogViewerDialog : ContentDialog
    {
        public FullLogViewerDialog(IEnumerable logs)
        {
            InitializeComponent();
            DialogLogGrid.ItemsSource = logs;

            if (logs is INotifyCollectionChanged observable)
            {
                observable.CollectionChanged += (s, e) => 
                {
                    if (DialogLogGrid.Items.Count > 0)
                    {
                        DialogLogGrid.ScrollIntoView(DialogLogGrid.Items[DialogLogGrid.Items.Count - 1]);
                    }
                };
            }
        }
    }
}
