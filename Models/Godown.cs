using System;

namespace Acczite20.Models
{
    public class Godown : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string Parent { get; set; } = string.Empty;
    }
}
