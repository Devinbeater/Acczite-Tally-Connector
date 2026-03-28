using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;

using System.Diagnostics;
using Acczite20.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Acczite20.Models;

namespace Acczite20.Services.Sync
{
    public class MongoProjector : IMongoProjector
    {
        private readonly Channel<(string Collection, BsonDocument Document)> _channel;
        private readonly MongoService _mongoService;
        private readonly ILogger<MongoProjector> _log;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SyncStateMonitor _syncMonitor;
        private const int MaxQueueSize = 5000;

        public MongoProjector(MongoService mongoService, ILogger<MongoProjector> log, IServiceScopeFactory scopeFactory, SyncStateMonitor syncMonitor)
        {
            _mongoService = mongoService;
            _log = log;
            _scopeFactory = scopeFactory;
            _syncMonitor = syncMonitor;
            
            _channel = Channel.CreateBounded<(string, BsonDocument)>(new BoundedChannelOptions(MaxQueueSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true 
            });
        }

        public void Project(string collection, BsonDocument document)
        {
            if (!_channel.Writer.TryWrite((collection, document)))
            {
                // Channel full (5000 items in memory). Spill to durable SQL storage.
                _log.LogWarning("Memory projection channel full. Spilling to durable SQL fallback for collection: {Collection}", collection);
                Task.Run(() => SpillToFallbackAsync(collection, document));
            }
        }

        private async Task SpillToFallbackAsync(string collection, BsonDocument document)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                
                db.MongoProjectionQueue.Add(new MongoProjectionQueue
                {
                    CollectionName = collection,
                    PayloadJson = document.ToJson(),
                    DetectedAt = DateTimeOffset.UtcNow
                });
                
                await db.SaveChangesAsync();
                _log.LogInformation("Successfully spilled to SQL fallback for {Collection}", collection);
            }
            catch (Exception ex)
            {
                _log.LogCritical(ex, "CRITICAL LOSS: Failed to spill to SQL fallback. Data for collection {Collection} may be lost.", collection);
            }
        }

        public async Task DrainFallbackQueueAsync(CancellationToken ct)
        {
            _log.LogInformation("Fallback Queue Drainer started.");
            
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    
                    var pending = await db.MongoProjectionQueue
                        .OrderBy(x => x.CreatedAt)
                        .Take(100)
                        .ToListAsync(ct);

                    if (!pending.Any())
                    {
                        _syncMonitor.MongoQueueDepth = 0;
                        await Task.Delay(30000, ct); // Poll every 30s
                        continue;
                    }

                    _syncMonitor.MongoQueueDepth = pending.Count;

                    foreach (var item in pending)
                    {
                        var doc = BsonDocument.Parse(item.PayloadJson);
                        // Blocking write because drainer is a background thread and we want to respect memory limits
                        await _channel.Writer.WriteAsync((item.CollectionName, doc), ct);
                        
                        db.MongoProjectionQueue.Remove(item);
                        await db.SaveChangesAsync(ct);
                    }
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    _log.LogError(ex, "Error draining fallback queue. Waiting 60s.");
                    await Task.Delay(60000, ct);
                }
            }
        }

        public void ProjectBatch(string collection, IEnumerable<BsonDocument> documents)
        {
            foreach (var doc in documents) Project(collection, doc);
        }

        public async Task ProcessQueueAsync(CancellationToken ct)
        {
            _log.LogInformation("Mongo Projector consumption loop started.");
            var stopwatch = new Stopwatch();
            
            await foreach (var (collection, doc) in _channel.Reader.ReadAllAsync(ct))
            {
                stopwatch.Restart();
                int retryDelay = 2000;
                bool success = false;

                while (!success && !ct.IsCancellationRequested)
                {
                    try
                    {
                        await _mongoService.UpsertDocumentAsync(collection, doc);
                        success = true;
                        
                        _syncMonitor.LastMongoProjectedAt = DateTime.Now;
                        _syncMonitor.MongoProjectionLagMs = stopwatch.ElapsedMilliseconds;
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning("Transient Mongo error for {Collection}: {Msg}. Retrying in {Delay}ms.", collection, ex.Message, retryDelay);
                        await Task.Delay(retryDelay, ct);
                        retryDelay = Math.Min(60000, retryDelay * 2); // Exponential backoff up to 1 min
                    }
                }
            }
        }
    }
}
