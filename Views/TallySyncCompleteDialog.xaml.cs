using System;
using ModernWpf.Controls;

namespace Acczite20.Views
{
    public partial class TallySyncCompleteDialog
    {
        public TallySyncCompleteDialog()
        {
            InitializeComponent();
        }

        public void SetResults(long records, double rate)
        {
            RecordsText.Text = records.ToString("N0");
            RateText.Text = $"{rate:N0} /sec";
        }
    }
}
