using ECommon.Components;
using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.SqlServer
{
    public static class ENodeExtensions
    {
        /// <summary>Use the SqlServerEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseSqlServerEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, SqlServerEventStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the SqlServerPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseSqlServerPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, SqlServerPublishedVersionStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the SqlServerLockService as the ILockService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseSqlServerLockService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ILockService, SqlServerLockService>();
            return eNodeConfiguration;
        }

        public static ENodeConfiguration UseSqlServerErrorStoreService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IErrorStore, SqlServerErrorStore>();
            return eNodeConfiguration;
        }
        /// <summary>Initialize the SqlServerEventStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="versionIndexName"></param>
        /// <param name="commandIndexName"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="bulkCopyTimeoutSeconds"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeSqlServerEventStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "EventStream",
            int tableCount = 1,
            string versionIndexName = "IX_EventStream_AggId_Version",
            string commandIndexName = "IX_EventStream_AggId_CommandId",
            int bulkCopyBatchSize = 1000,
            int bulkCopyTimeoutSeconds = 60)
        {
            ((SqlServerEventStore)ObjectContainer.Resolve<IEventStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                versionIndexName,
                commandIndexName,
                bulkCopyBatchSize,
                bulkCopyTimeoutSeconds);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the SqlServerPublishedVersionStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="uniqueIndexName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeSqlServerPublishedVersionStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "PublishedVersion",
            int tableCount = 1,
            string uniqueIndexName = "IX_PublishedVersion_AggId_Version")
        {
            ((SqlServerPublishedVersionStore)ObjectContainer.Resolve<IPublishedVersionStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                uniqueIndexName);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the SqlServerLockService with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeSqlServerLockService(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "LockKey")
        {
            ((SqlServerLockService)ObjectContainer.Resolve<ILockService>()).Initialize(connectionString, tableName);
            return eNodeConfiguration;
        }

        public static ENodeConfiguration InitializeSqlServerErrorStore(this ENodeConfiguration eNodeConfiguration, string connectionString)
        {
            ((SqlServerErrorStore)ObjectContainer.Resolve<IErrorStore>()).Initialize(connectionString);
            return eNodeConfiguration;
        }
    }
}