using ECommon.Components;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace ENode.Domain.Impl
{
    public class DefaultRepository : IRepository
    {
        private readonly IMemoryCache _memoryCache;

        public DefaultRepository(IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
        }

        public async Task<T> GetAsync<T>(object aggregateRootId) where T : class, IAggregateRoot
        {
            return await GetAsync(typeof(T), aggregateRootId).ConfigureAwait(false) as T;
        }
        public async Task<IAggregateRoot> GetAsync(Type aggregateRootType, object aggregateRootId)
        {
            if (aggregateRootType == null)
            {
                throw new ArgumentNullException("aggregateRootType");
            }
            if (aggregateRootId == null)
            {
                throw new ArgumentNullException("aggregateRootId");
            }
            var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity("DefaultRepository.GetAsync");
            act?.SetTag("AggregateRootId", aggregateRootId);
            var aggregateRoot = await _memoryCache.GetAsync(aggregateRootId, aggregateRootType).ConfigureAwait(false);
            if (aggregateRoot == null)
            {
                act?.SetTag("DefaultRepository", "RefreshAggregateFromEventStoreAsync");
                aggregateRoot = await _memoryCache.RefreshAggregateFromEventStoreAsync(aggregateRootType, aggregateRootId.ToString()).ConfigureAwait(false);
            }
            else
            {
                act?.SetTag("DefaultRepository", "InMemory");
            }
            act?.Stop();
            return aggregateRoot;
        }
    }
}
