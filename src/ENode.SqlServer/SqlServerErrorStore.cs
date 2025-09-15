using ECommon.Components;
using ECommon.Dapper;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Infrastructure;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading;

namespace ENode.SqlServer
{
    public class SqlServerErrorStore : IErrorStore
    {
        private string _connectionString;
        private ILogger _logger;
        private ITimeProvider _timeProvider;
        private IOHelper _ioHelper;
        private IJsonSerializer _jsonSerializer;

        public SqlServerErrorStore Initialize(string connectionString)
        {
            _connectionString = connectionString;

            Ensure.NotNull(_connectionString, "_connectionString");

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _timeProvider = ObjectContainer.Resolve<ITimeProvider>();
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();

            return this;
        }

        public void SaveCommandError(string aggregateRootId, string command, string commandException, string messageId)
        {
            WriteDB(new { aggregateRootId, CommandName = command, commandException, CommandMessageID = messageId, CommandErrorTime = _timeProvider.GetCurrentTime() }, new { aggregateRootId });
        }

        public void SaveEventError(string aggregateRootId, string eventException, int? version, string messageId)
        {
            WriteDB(new { aggregateRootId, eventException, EventMessageID = messageId, version, EventErrorTime = _timeProvider.GetCurrentTime() }, new { aggregateRootId });
        }

        private void WriteDB(object data, object condition)
        {
            ThreadPool.QueueUserWorkItem(state =>
             {
                 using (var conn = GetConnection())
                 {
                     int row = 0;
                     try
                     {
                         conn.Open();
                         row = conn.Update(data, condition, "HandleErrorLog");
                     }
                     catch (Exception ex)
                     {
                         _logger.Error(ex.ToString());
                     }
                     if (row == 0)
                     {
                         try
                         {
                             conn.Insert(data, "HandleErrorLog");
                         }
                         catch (SqlException ex)
                         {
                             if (ex.Number != 2601) _logger.Error(_jsonSerializer.Serialize(data) + "\n" + ex.ToString());
                         }
                         catch (Exception ex)
                         {
                             _logger.Error(ex.ToString());
                         }
                     }
                 }
             });
        }

        private SqlConnection GetConnection()
        {
            return new SqlConnection(_connectionString);
        }
    }
}
