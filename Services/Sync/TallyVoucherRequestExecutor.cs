using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Acczite20.Models;
using Acczite20.Services;

namespace Acczite20.Services.Sync
{
    public sealed class TallyVoucherRequestExecutor
    {
        private readonly TallyXmlService _tallyService;
        private readonly TallyXmlParser _xmlParser;
        private readonly VoucherSyncChunkScheduler _scheduler;

        public TallyVoucherRequestExecutor(
            TallyXmlService tallyService,
            TallyXmlParser xmlParser,
            VoucherSyncChunkScheduler scheduler)
        {
            _tallyService = tallyService;
            _xmlParser = xmlParser;
            _scheduler = scheduler;
        }

        public async IAsyncEnumerable<Voucher> ExportVouchersStreamAsync(
            Guid orgId,
            Guid companyId,
            DateRange range,
            VoucherChunkExecutionMetrics metrics,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            metrics.WindowUsed = _scheduler.CurrentWindow;

            await using var lease = await _tallyService.OpenCollectionXmlStreamAsync(
                "AccziteVoucherPipeline",
                range.Start,
                range.End,
                true,
                ct);

            if (lease == null)
            {
                metrics.Elapsed = sw.Elapsed;
                _scheduler.Adjust(metrics.Elapsed, metrics.FetchedCount, 0, failed: true);
                throw new InvalidOperationException($"Tally did not return a voucher stream for {range}.");
            }

            metrics.PayloadBytes = lease.DeclaredSize;

            var failed = true;
            try
            {
                var settings = new XmlReaderSettings
                {
                    Async = true,
                    CheckCharacters = false,
                    IgnoreWhitespace = true,
                    CloseInput = false
                };

                using var reader = XmlReader.Create(lease.Stream, settings);

                while (await reader.ReadAsync())
                {
                    ct.ThrowIfCancellationRequested();

                    if (reader.NodeType != XmlNodeType.Element ||
                        !string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    XElement element;
                    try
                    {
                        element = (XElement)XNode.ReadFrom(reader);
                    }
                    catch
                    {
                        metrics.RejectedCount++;
                        continue;
                    }

                    var voucher = _xmlParser.ParseVoucherEntity(element, orgId, companyId);
                    if (voucher == null)
                    {
                        metrics.RejectedCount++;
                        continue;
                    }

                    metrics.FetchedCount++;
                    yield return voucher;
                }

                failed = false;
            }
            finally
            {
                sw.Stop();
                metrics.Elapsed = sw.Elapsed;
                _scheduler.Adjust(metrics.Elapsed, metrics.FetchedCount, metrics.PayloadBytes, failed);
            }
        }
    }
}
