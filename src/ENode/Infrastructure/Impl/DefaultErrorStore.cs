using System;

namespace ENode.Infrastructure.Impl
{
    public class DefAultErrorStore : IErrorStore
    {
        public void SaveCommandError(string aggregateRootId, string command, string commandException, string messageId)
        {
            Console.WriteLine("Command Error: AggregateRootId={0}, Command={1}, Exception={2}, MessageId={3}", aggregateRootId, command, commandException, messageId);
        }

        public void SaveEventError(string aggregateRootId, string eventException, int? version, string messageId)
        {
            Console.WriteLine("Event Error: AggregateRootId={0}, Exception={1}, Version={2}, MessageId={3}", aggregateRootId, eventException, version, messageId);
        }
    }
}