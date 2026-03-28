using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Acczite20.Models;

namespace Acczite20.Services.Tally
{
    public class TallyMasterDto
    {
        public string Name { get; set; } = string.Empty;
        public string? Parent { get; set; }
        public string? TallyMasterId { get; set; }
        public long TallyAlterId { get; set; }
        public Dictionary<string, string> Properties { get; set; } = new();
    }
}
