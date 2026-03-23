using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Xml.Linq;
using Acczite20.Services.Sync;

namespace Acczite20.Services.Tally
{
    public class TallyCompanyService
    {
        private readonly TallyXmlService _tallyXml;

        public TallyCompanyService(TallyXmlService tallyXml)
        {
            _tallyXml = tallyXml;
        }

        public async Task<string?> GetOpenCompanyAsync()
        {
            var primaryResponse = await _tallyXml.SendEnvelopeAsync(TallyXmlEnvelopeBuilder.BuildCompanyList());
            var companyName = ParseCompanyName(primaryResponse);
            if (!string.IsNullOrWhiteSpace(companyName))
            {
                return companyName;
            }

            // Some Tally builds reject the report-style envelope but return the company via collection export.
            var fallbackResponse = await _tallyXml.SendEnvelopeAsync(TallyXmlEnvelopeBuilder.BuildCompanyCollectionRequest());
            return ParseCompanyName(fallbackResponse);
        }

        private static string SanitizeXml(string? xml)
        {
            if (string.IsNullOrEmpty(xml)) return string.Empty;
            // Filter illegal XML chars: Tally data often contains 0x04 (EOT), 0x00, etc.
            // We keep allowed whitespace: \r, \n, \t
            return new string(xml.Where(c => 
                (c >= 0x20 && c <= 0xD7FF) || 
                c == 0x09 || c == 0x0A || c == 0x0D || 
                (c >= 0xE000 && c <= 0xFFFD)
            ).ToArray());
        }

        private static string? ParseCompanyName(string? content)
        {
            if (string.IsNullOrWhiteSpace(content) ||
                content.IndexOf("<LINEERROR>", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return null;
            }

            try
            {
                // Loose parsing allows illegal 0x04 chars from Tally to pass without crashing
                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var reader = System.Xml.XmlReader.Create(new System.IO.StringReader(content), settings);
                var doc = XDocument.Load(reader);
                
                var currentCompany = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("SVCURRENTCOMPANY", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim();

                if (IsUsableCompanyName(currentCompany))
                {
                    return currentCompany;
                }

                var companyNode = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("COMPANY", StringComparison.OrdinalIgnoreCase));

                var companyName = companyNode?.Elements()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim()
                    ?? companyNode?.Attributes()
                        .FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                        ?.Value
                        ?.Trim();

                if (IsUsableCompanyName(companyName))
                {
                    return companyName;
                }

                var companyTag = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("COMPANYNAME", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim();

                if (IsUsableCompanyName(companyTag))
                {
                    return companyTag;
                }

                return doc.Descendants()
                    .Where(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Value?.Trim())
                    .FirstOrDefault(IsUsableCompanyName);
            }
            catch
            {
                return null;
            }
        }

        private static bool IsUsableCompanyName(string? companyName)
        {
            return !string.IsNullOrWhiteSpace(companyName)
                && !string.Equals(companyName, "None", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(companyName, "Default Company", StringComparison.OrdinalIgnoreCase);
        }
    }
}
