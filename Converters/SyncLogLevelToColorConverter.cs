using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace Acczite20.Converters
{
    public class SyncLogLevelToColorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string level)
            {
                return level.ToUpper() switch
                {
                    "ERROR" => Brushes.IndianRed,
                    "WARNING" => Brushes.DarkOrange,
                    "SUCCESS" => Brushes.MediumSeaGreen,
                    "DEBUG" => Brushes.LightSlateGray,
                    _ => Brushes.Transparent // Default to transparent (item container will handle inherited text color)
                };
            }
            return Brushes.Transparent;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class SyncLogLevelToForegroundConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string level)
            {
                return level.ToUpper() switch
                {
                    "ERROR" => Brushes.White,
                    "WARNING" => Brushes.White,
                    "SUCCESS" => Brushes.White,
                    "DEBUG" => Brushes.White,
                    _ => Brushes.Black
                };
            }
            return Brushes.Black;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
