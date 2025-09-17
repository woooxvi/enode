using ECommon.Components;
using ENode.Configurations;
using ENode.Domain;
using System;

namespace ENode.AggregateSnapshotStore
{
    /// <summary>
    /// AggregateSnapshotStore enode extensions
    /// </summary>
    public static class ENodeExtensions
    {
        /// <summary>
        /// Use AggregateSnapshotStore
        /// </summary>
        /// <typeparam name="TAggregateSnapshotStore"></typeparam>
        /// <param name="enodeConfiguration"></param>
        /// <returns></returns>
        public static ENodeConfiguration UseAggregateSnapshotStore(this ENodeConfiguration enodeConfiguration)
        {
            var configuration = enodeConfiguration.GetCommonConfiguration();
            configuration.SetDefault<IAggregateSnapshotter, MyAggregateSnapshotter>((string)null);
            return enodeConfiguration;
        }
    }
}
