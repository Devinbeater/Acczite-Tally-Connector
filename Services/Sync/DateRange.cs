using System;

namespace Acczite20.Services.Sync
{
    public readonly record struct DateRange(DateTimeOffset Start, DateTimeOffset End)
    {
        public override string ToString() => $"{Start:yyyy-MM-dd HH:mm:ss} -> {End:yyyy-MM-dd HH:mm:ss}";
    }
}
