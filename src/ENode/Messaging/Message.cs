using System;
using System.Collections.Generic;
using System.Diagnostics;
using ECommon.Utilities;

namespace ENode.Messaging
{
    /// <summary>Represents an abstract message.
    /// </summary>
    [Serializable]
    public abstract class Message : IMessage
    {
        /// <summary>Represents the identifier of the message.
        /// </summary>
        public string Id { get; set; }
        /// <summary>Represents the timestamp of the message.
        /// </summary>
        public DateTime Timestamp { get; set; }
        /// <summary>Represents the extension key/values data of the message.
        /// </summary>
        public IDictionary<string, string> Items { get; set; }
        private Activity activity = null;
        /// <summary>
        /// 用于追溯的源
        /// </summary>
        public Activity GetActivity() { return activity; }
        /// <summary>
        /// 设置追踪源
        /// </summary>
        /// <param name="activity"></param>
        public void SetActivity(Activity activity, string messageId, string aggregateRootId)
        {
            this.activity = activity;
            activity?.AddTag("messageId", messageId);
            activity?.SetTag("AggregateRootId", aggregateRootId);
        }

        /// <summary>Default constructor.
        /// </summary>
        public Message()
        {
            Id = ObjectId.GenerateNewStringId();
            Timestamp = DateTime.Now;
            Items = new Dictionary<string, string>();
        }

        public void MergeItems(IDictionary<string, string> items)
        {
            if (items == null || items.Count == 0)
            {
                return;
            }
            if (Items == null)
            {
                Items = new Dictionary<string, string>();
            }
            foreach (var entry in items)
            {
                if (!Items.ContainsKey(entry.Key))
                {
                    Items.Add(entry.Key, entry.Value);
                }
            }
        }

    }
}
