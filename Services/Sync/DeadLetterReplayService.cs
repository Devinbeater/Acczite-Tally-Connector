using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Acczite20.Services.Sync
{
    public class DeadLetterReplayService
    {
        private readonly AppDbContext _context;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<DeadLetterReplayService> _log;

        private readonly SyncStateMonitor _syncMonitor;

        public DeadLetterReplayService(
            AppDbContext context,
            IServiceScopeFactory scopeFactory,
            SyncStateMonitor syncMonitor,
            ILogger<DeadLetterReplayService> log)
        {
            _context = context;
            _scopeFactory = scopeFactory;
            _syncMonitor = syncMonitor;
            _log = log;
        }

        public async Task ReplayAsync(Guid orgId, CancellationToken ct = default)
        {
            _log.LogInformation("Starting Dead-Letter Replay for Org: {Org}", orgId);
            
            var pending = await _context.DeadLetters
                .Where(d => d.OrganizationId == orgId 
                       && !d.IsResolved 
                       && d.RetryCount < 10
                       && (d.FailureType == DeadLetterFailureType.MissingMaster || d.FailureType == DeadLetterFailureType.Transient))
                .OrderBy(d => d.DetectedAt)
                .ToListAsync(ct);

            _syncMonitor.ReplayQueueSize = pending.Count;
            if (!pending.Any()) return;

            _log.LogInformation("Found {Count} pending vouchers in Dead-Letter queue.", pending.Count);

            foreach (var letter in pending)
            {
                try
                {
                    letter.LastAttemptAt = DateTimeOffset.UtcNow;
                    letter.RetryCount++;

                    if (string.IsNullOrEmpty(letter.PayloadXml))
                    {
                        letter.IsResolved = true;
                        letter.ErrorReason = "Payload missing, cannot replay.";
                        continue;
                    }

                    // Attempt to process the voucher again
                    using var scope = _scopeFactory.CreateScope();
                    var orchestrator = scope.ServiceProvider.GetRequiredService<TallySyncOrchestrator>();
                    var success = await orchestrator.ProcessVoucherXmlAsync(orgId, letter.PayloadXml);
                    
                    if (success)
                    {
                        letter.IsResolved = true;
                        _log.LogInformation("Successfully replayed voucher {VoucherId}.", letter.TallyMasterId);
                    }
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Replay failed for voucher {VoucherId}.", letter.TallyMasterId);
                }
            }

            await _context.SaveChangesAsync(ct);
        }
    }
}
