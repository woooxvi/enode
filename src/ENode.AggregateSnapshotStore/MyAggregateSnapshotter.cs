using ECommon.IO;
using ENode.Domain;
using ENode.Domain.Impl;
using ENode.Eventing;
using ENode.Infrastructure;
using LeraySportAmateurMatch.DAL.Memory.AggregateSnapshotStore;
using LeraySportAmateurMatch.DAL.Redis;
using LeraySportAmateurMatch.SystemContract;
using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshotStore
{
    /// <summary>
    /// 聚合仓储基类，用于快照获取和生成
    /// </summary>
    public class MyAggregateSnapshotter : DefaultAggregateSnapshotter, IAggregateSnapshotter
    {

        /// <summary>
        /// 聚合仓储基类，用于快照获取和生成
        /// </summary>
        public MyAggregateSnapshotter(IAggregateRepositoryProvider aggregateRepositoryProvider, IOHelper ioHelper) : base(aggregateRepositoryProvider, ioHelper)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="aggregateRootTypeName"></param>
        /// <param name="aggregateRootId"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        public new Task UpdateSnapshotAsync(string aggregateRootTypeName, string aggregateRootId, int version)
        {
            RedisHelper.GetInstance(11).StringSet(RedisPrefixKeys.AGGREGATESNAPSHOT + DateTime.Now.ToString("yyyyMMdd") + "_" + aggregateRootId, new AggregateSnapshotHeader(aggregateRootId, aggregateRootTypeName, version), TimeSpan.FromDays(2));
            return Task.CompletedTask;
        }
    }
}
