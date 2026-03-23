using System;

namespace Acczite20.Models
{
    public class Currency : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string FormalName { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
    }
}
