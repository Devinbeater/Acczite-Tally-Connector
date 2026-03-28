using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace Acczite20.Services.Sync
{
    public interface IMongoProjector
    {
        void Project(string collection, BsonDocument document);
        void ProjectBatch(string collection, IEnumerable<BsonDocument> documents);
        Task ProcessQueueAsync(CancellationToken ct);
        Task DrainFallbackQueueAsync(CancellationToken ct);
    }
}
