using System;
using System.Threading.Tasks;
using ENode.Domain;

namespace ENode.Domain
{
    /// <summary>An interface which can restore aggregate from snapshot storage.
    /// </summary>
    public interface IAggregateSnapshotter
    {
        /// <summary>Restore the aggregate from snapshot storage.
        /// </summary>
        Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId);
        /// <summary>
        /// Update the aggregate snapshot
        /// </summary>
        /// <param name="aggregateRootTypeName">类型名称</param>
        /// <param name="aggregateRootId">聚合根ID</param>
        /// <param name="version">版本号</param>
        /// <returns></returns>
        Task UpdateSnapshotAsync(string aggregateRootTypeName, string aggregateRootId, int version);
    }
}
