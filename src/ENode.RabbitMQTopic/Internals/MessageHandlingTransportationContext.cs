using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQTopic.Internals
{
    internal class MessageHandlingTransportationContext : IMessageTransportationContext
    {
        private readonly IChannel _channel;
        private readonly ulong _deliveryTag;

        public MessageHandlingTransportationContext(string topic, int queueIndex, string groupName, IChannel channel,
            ulong deliveryTag, IDictionary<string, object> properties)
        {
            Topic = topic;
            QueueIndex = queueIndex;
            GroupName = groupName;
            _channel = channel;
            _deliveryTag = deliveryTag;
            Properties = properties;
        }

        public string Topic { get; private set; }

        public int QueueIndex { get; private set; }

        public string GroupName { get; private set; }

        public IDictionary<string, object> Properties { get; private set; }

        public event EventHandler OnAck;

        public async void Ack()
        {
            await _channel.BasicAckAsync(_deliveryTag, false);
            OnAck?.Invoke(this, new EventArgs());
        }
    }
}
