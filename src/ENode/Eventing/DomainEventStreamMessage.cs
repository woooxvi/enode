using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ECommon.Components;
using ENode.Messaging;

namespace ENode.Eventing
{
    [Serializable]
    public class DomainEventStreamMessage : Message
    {
        public string AggregateRootId { get; set; }
        public string AggregateRootTypeName { get; set; }
        public int Version { get; private set; }
        public string CommandId { get; set; }
        public IEnumerable<IDomainEvent> Events { get; set; }
        public Activity NextActivity(string name)
        {
            var activity = GetActivity();
            if (activity != null)
            {
                if (ObjectContainer.TryResolve(out ActivitySource source))
                {
                    var act = source.StartActivity(ActivityKind.Internal, activity.Context, name: name);
                    act.SetTag("AggregateRootId", AggregateRootId);
                }
            }
            return null;
        }
        public DomainEventStreamMessage() { }
        public DomainEventStreamMessage(string commandId, string aggregateRootId, int version, string aggregateRootTypeName, IEnumerable<IDomainEvent> events, IDictionary<string, string> items)
        {
            CommandId = commandId;
            AggregateRootId = aggregateRootId;
            Version = version;
            AggregateRootTypeName = aggregateRootTypeName;
            Events = events;
            Items = items;
        }

        public override string ToString()
        {
            return string.Format("[Id={0},CommandId={1},AggregateRootId={2},AggregateRootTypeName={3},Version={4},Events={5},Items={6},Timestamp={7}]",
                Id,
                CommandId,
                AggregateRootId,
                AggregateRootTypeName,
                Version,
                string.Join("|", Events.Select(x => x.GetType().Name)),
                string.Join("|", Items.Select(x => x.Key + ":" + x.Value)),
                Timestamp);
        }
    }
}
