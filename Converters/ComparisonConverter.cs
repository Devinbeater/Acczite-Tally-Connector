using System;
using System.Globalization;
using System.Windows.Data;

namespace Acczite20.Converters
{
    public class ComparisonConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is decimal d)
            {
                string mode = parameter as string;
                if (mode == "IsPositive") return d > 0;
                if (mode == "IsNegative") return d < 0;
            }
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
