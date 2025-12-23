using System;
using System.Diagnostics;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Eventing;
using RabbitMQTopic;

namespace ENode.RabbitMQ
{
    /// <summary>
    /// DomainEvent Consumer
    /// </summary>
    public class DomainEventConsumer
    {
        private SendReplyService _sendReplyService;
        private IJsonSerializer _jsonSerializer;
        private IEventSerializer _eventSerializer;
        private IProcessingEventProcessor _messageProcessor;
        private ILogger _logger;
        private bool _sendEventHandledMessage;
        private Consumer _consumer;

        /// <summary>
        /// Initialize ENode
        /// </summary>
        /// <param name="sendEventHandledMessage"></param>
        /// <returns></returns>
        public DomainEventConsumer InitializeENode(ConsumerSettings settings, bool sendEventHandledMessage = true)
        {
            _sendReplyService = new SendReplyService("EventConsumerSendReplyService", settings);
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _eventSerializer = ObjectContainer.Resolve<IEventSerializer>();
            _messageProcessor = ObjectContainer.Resolve<IProcessingEventProcessor>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendEventHandledMessage = sendEventHandledMessage;
            return this;
        }

        /// <summary>
        /// Initialize RabbitMQ
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="autoConfig"></param>
        /// <param name="sendEventHandledMessage"></param>
        /// <returns></returns>
        public DomainEventConsumer InitializeRabbitMQ(ConsumerSettings settings, bool autoConfig = true, bool sendEventHandledMessage = true)
        {
            InitializeENode(settings, sendEventHandledMessage);
            _consumer = new Consumer(settings, autoConfig);
            return this;
        }

        /// <summary>
        /// Start
        /// </summary>
        /// <returns></returns>
        public DomainEventConsumer Start()
        {
            _sendReplyService.Start();
            _consumer.OnMessageReceived += (sender, e) =>
            {
                try
                {
                    var message = _jsonSerializer.Deserialize<EventStreamMessage>(Encoding.UTF8.GetString(e.Context.GetBody()));
                    var domainEventStreamMessage = ConvertToDomainEventStream(message);
                    var processContext = new DomainEventStreamProcessContext(this, domainEventStreamMessage, e.Context);
                    var processingMessage = new ProcessingEvent(domainEventStreamMessage, processContext);
                    _logger.DebugFormat("ENode event stream message received, messageId: {0}, aggregateRootId: {1}, aggregateRootType: {2}, version: {3}", domainEventStreamMessage.Id, domainEventStreamMessage.AggregateRootId, domainEventStreamMessage.AggregateRootTypeName, domainEventStreamMessage.Version);
                    _messageProcessor.Process(processingMessage);
                }
                catch (Exception ex)
                {
                    _logger.Error($"ENode event stream message handle failed: {ex.Message}, eventStream: {Encoding.UTF8.GetString(e.Context.GetBody())}", ex);
                }
            };
            _consumer.Start();
            return this;
        }

        /// <summary>
        /// Subscribe topic
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueCount"></param>
        /// <returns></returns>
        public DomainEventConsumer Subscribe(string topic, int queueCount = 4)
        {
            _consumer.Subscribe(topic, queueCount);
            return this;
        }

        /// <summary>
        /// Shutdown
        /// </summary>
        /// <returns></returns>
        public DomainEventConsumer Shutdown()
        {
            _consumer.Shutdown();
            _sendReplyService.Stop();
            return this;
        }

        private DomainEventStreamMessage ConvertToDomainEventStream(EventStreamMessage message)
        {
            var domainEventStreamMessage = new DomainEventStreamMessage(
                message.CommandId,
                message.AggregateRootId,
                message.Version,
                message.AggregateRootTypeName,
                _eventSerializer.Deserialize<IDomainEvent>(message.Events),
                message.Items)
            {
                Id = message.Id,
                Timestamp = message.Timestamp
            };
            return domainEventStreamMessage;
        }

        class DomainEventStreamProcessContext : IEventProcessContext
        {
            private readonly IMessageTransportationContext _messageContext;
            private readonly DomainEventConsumer _eventConsumer;
            private readonly DomainEventStreamMessage _domainEventStreamMessage;
            private ILogger _logger;

            public DomainEventStreamProcessContext(DomainEventConsumer eventConsumer, DomainEventStreamMessage domainEventStreamMessage, IMessageTransportationContext messageContext)
            {
                _messageContext = messageContext;
                _eventConsumer = eventConsumer;
                _domainEventStreamMessage = domainEventStreamMessage;
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            }

            public string QueueMessageId => _domainEventStreamMessage.Id;

            public void NotifyEventProcessed()
            {
                var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity("DomainEventStreamProcessContext.NotifyEventProcessed");
                _messageContext.Ack();
                _logger.Info("NotifyEventProcessed equeue message:" + _domainEventStreamMessage);
                if (!_eventConsumer._sendEventHandledMessage)
                {
                    return;
                }

                if (!_domainEventStreamMessage.Items.TryGetValue("CommandReplyAddress", out string replyAddress) || string.IsNullOrEmpty(replyAddress))
                {
                    return;
                }
                _domainEventStreamMessage.Items.TryGetValue("CommandResult", out string commandResult);

                act?.SetTag("CommandId", _domainEventStreamMessage.CommandId);
                act?.SetTag("AggregateRootId", _domainEventStreamMessage.AggregateRootId);
                _eventConsumer._sendReplyService.SendReply((int)CommandReturnType.EventHandled, new DomainEventHandledMessage
                {
                    CommandId = _domainEventStreamMessage.CommandId,
                    AggregateRootId = _domainEventStreamMessage.AggregateRootId,
                    CommandResult = commandResult
                }, replyAddress, _domainEventStreamMessage.CommandId);
                act?.Stop();
            }
        }
    }
}
