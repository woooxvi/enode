using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQTopic.Internals;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using IRabbitMQConnection = RabbitMQ.Client.IConnection;
using RabbitMQConnectionFactory = RabbitMQ.Client.ConnectionFactory;

namespace RabbitMQTopic
{
    /// <summary>
    /// 消费者
    /// </summary>
    public class Consumer
    {
        private readonly Uri _amqpUri = null;
        private readonly string _clientName = null;
        private IRabbitMQConnection _amqpConnection = null;
        private bool _selfCreate = false;
        private readonly ConsumeMode _mode;
        private readonly ushort _prefetchCount = 0;
        private readonly string _groupName = null;
        private readonly int _consumerCount = 0;
        private readonly int _consumerSequence = 0;
        private readonly bool _autoConfig = false;
        private readonly Dictionary<string, int> _topics = new Dictionary<string, int>();

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, (IChannel channel, Thread thread)>> _globalChannels = new();

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, AsyncEventingBasicConsumer>>
            _globalConsumers = new ConcurrentDictionary<string, ConcurrentDictionary<int, AsyncEventingBasicConsumer>>();
        private volatile int _isRunning = 0;

        private const ushort ChannelError = 504;
        private const ushort ConnectionForced = 320;

        /// <summary>
        /// 消费者ClientName
        /// </summary>
        public string ClientName => _clientName;
        /// <summary>
        /// 消息已接受事件
        /// </summary>
        public event EventHandler<MessageReceivedEventArgs> OnMessageReceived;

        /// <summary>
        /// 消费者
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="autoConfig">自动建Exchange、Queue和Bind</param>
        public Consumer(ConsumerSettings settings, bool autoConfig = true)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (settings.AmqpConnection == null && settings.AmqpUri == null)
            {
                throw new ArgumentException("AmqpConnection or AmqpUri must be set.");
            }

            if (settings.GroupName == "default")
            {
                throw new ArgumentException("GroupName cann't use reserve keywords \"default\".");
            }

            _amqpUri = settings.AmqpUri;
            if (settings.AmqpConnection != null)
            {
                _amqpConnection = settings.AmqpConnection;
                _clientName = settings.AmqpConnection.ClientProvidedName;
            }

            _mode = settings.Mode;
            _prefetchCount = settings.PrefetchCount <= 0 ? (ushort)1 : (ushort)settings.PrefetchCount;
            _groupName = settings.GroupName ?? string.Empty;
            _consumerCount = settings.ConsumerCount <= 0 ? 1 : settings.ConsumerCount;
            _consumerSequence = settings.ConsumerSequence <= 0 || settings.ConsumerSequence > _consumerCount
                ? 1
                : settings.ConsumerSequence;
            _autoConfig = autoConfig;
        }

        /// <summary>
        /// 订阅Topic
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueCount">Topic的队列数（必须为2的幂）</param>
        /// <return></return>
        public Consumer Subscribe(string topic, int queueCount)
        {
            if (_isRunning == 1)
            {
                throw new NotSupportedException("Couldn't subscribe topic when is running.");
            }

            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentNullException(nameof(topic), "must not empty.");
            }

            if (queueCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(queueCount), queueCount,
                    "QueueCount must greater than zero.");
            }

            if ((queueCount & (queueCount - 1)) != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(queueCount), queueCount,
                    "QueueCount must be the power of 2.");
            }

            if (!_topics.ContainsKey(topic))
            {
                _topics.Add(topic, queueCount);
            }

            return this;
        }

        /// <summary>
        /// 启动
        /// </summary>
        public void Start()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 1, 0) == 0)
            {
                if (_amqpConnection == null)
                {
                    var connFactory = new RabbitMQConnectionFactory
                    {
                        Uri = _amqpUri
                    };
                    _amqpConnection = connFactory.CreateConnectionAsync(_clientName).Result;
                    _selfCreate = true;
                }

                if (_autoConfig)
                {
                    using (var channelForConfig = _amqpConnection.CreateChannelAsync().Result)
                    {
                        foreach (var topic in _topics.Keys)
                        {
                            var queueCount = _topics[topic];
                            var subTopic = GetSubTopic(topic);
                            channelForConfig.ExchangeDeclareAsync(topic, ExchangeType.Fanout, true, false, null).Wait();
                            channelForConfig.ExchangeDeclareAsync(subTopic, ExchangeType.Direct, true, false, null).Wait();
                            channelForConfig.ExchangeBindAsync(subTopic, topic, "", null).Wait();

                            for (int queueIndex = 0; queueIndex < queueCount; queueIndex++)
                            {
                                string queueName = GetQueue(topic, queueIndex);
                                channelForConfig.QueueDeclareAsync(queueName, true, false, false, null).Wait();
                                channelForConfig.QueueBindAsync(queueName, subTopic, queueIndex.ToString(), null).Wait();
                            }
                        }

                        channelForConfig.CloseAsync().Wait();
                    }
                }

                if (_mode == ConsumeMode.Push)
                {
                    ConsumeByPush();
                }
                else
                {
                    ConsumeByPull();
                }
            }
        }

        /// <summary>
        /// 关闭
        /// </summary>
        public void Shutdown()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 0, 1) == 1)
            {
                Thread.Sleep(100);
                if (_amqpConnection != null)
                {
                    if (_mode == ConsumeMode.Push)
                    {
                        foreach (var topic in _globalConsumers.Keys)
                        {
                            var consumers = _globalConsumers[topic];
                            foreach (var queueIndex in consumers.Keys)
                            {
                                var channel = consumers[queueIndex].Channel;
                                if (channel.IsOpen)
                                {
                                    channel.CloseAsync(ConnectionForced,
                                        $"\"{topic}-{queueIndex}\"'s normal channel disposed").Wait();
                                }
                            }

                            consumers.Clear();
                        }

                        _globalConsumers.Clear();
                    }
                    else
                    {
                        foreach (var topic in _globalChannels.Keys)
                        {
                            var channels = _globalChannels[topic];
                            foreach (var queueIndex in channels.Keys)
                            {
                                var channel = channels[queueIndex].Item1;
                                var consumerThread = channels[queueIndex].thread;
                                if (channel.IsOpen)
                                {
                                    channel.CloseAsync(ConnectionForced,
                                        $"\"{topic}-{queueIndex}\"'s normal channel disposed").Wait();
                                }
                            }

                            channels.Clear();
                        }

                        _globalChannels.Clear();
                    }

                    if (_selfCreate)
                    {
                        _amqpConnection.CloseAsync().Wait();
                        _amqpConnection = null;
                    }
                }
            }
        }

        /// <summary>
        /// 是否正在运行
        /// </summary>
        /// <value></value>
        public bool IsRunning => _isRunning == 1;

        private async void ConsumeByPush()
        {
            foreach (var topic in _topics.Keys)
            {
                var queueCount = _topics[topic];
                var subscribeQueues = GetSubscribeQueues(queueCount, _consumerCount, _consumerSequence).ToList();
                var subTopic = GetSubTopic(topic);
                var consumers = new ConcurrentDictionary<int, AsyncEventingBasicConsumer>();

                if (!_globalConsumers.TryAdd(subTopic, consumers))
                {
                    throw new Exception($"{subTopic} has subscribed.");
                }

                for (int queueIndex = 0; queueIndex < queueCount; queueIndex++)
                {
                    if (!subscribeQueues.Contains(queueIndex))
                    {
                        continue;
                    }

                    consumers.TryGetValue(queueIndex, out AsyncEventingBasicConsumer consumer);
                    if (consumer != null)
                    {
                        continue;
                    }

                    var channel = _amqpConnection.CreateChannelAsync().Result;
                    channel.BasicQosAsync(0, _prefetchCount, false).Wait();
                    consumer = new AsyncEventingBasicConsumer(channel);

                    try
                    {
                        consumer.ReceivedAsync += (sender, e) =>
                        {
                            var currentConsumer = ((AsyncEventingBasicConsumer)sender);
                            var currentChannel = currentConsumer.Channel;
                            while (consumers.All(w => w.Value.Channel != currentChannel))
                            {
                                Thread.Sleep(1000);
                            }

                            if (OnMessageReceived != null)
                            {
                                var currentTopic = e.Exchange.IndexOf("-delayed", StringComparison.Ordinal) > 0
                                    ? e.Exchange.Substring(0,
                                        e.Exchange.LastIndexOf("-delayed", StringComparison.Ordinal))
                                    : e.Exchange;
                                var currentQueueIndex = consumers.First(w => w.Value.Channel == currentChannel).Key;
                                var context = new MessageHandlingTransportationContext(currentTopic, currentQueueIndex,
                                    _groupName, channel, e.DeliveryTag, new Dictionary<string, object>
                                    {
                                        {MessagePropertyConstants.MESSAGE_ID, e.BasicProperties.MessageId},
                                        {MessagePropertyConstants.MESSAGE_TYPE, e.BasicProperties.Type},
                                        {
                                            MessagePropertyConstants.TIMESTAMP,
                                            e.BasicProperties.Timestamp.UnixTime == 0
                                                ? DateTime.Now
                                                : DateTime2UnixTime.FromUnixTime(e.BasicProperties.Timestamp.UnixTime)
                                        },
                                        {
                                            MessagePropertyConstants.CONTENT_TYPE,
                                            string.IsNullOrEmpty(e.BasicProperties.ContentType)
                                                ? "text/json"
                                                : e.BasicProperties.ContentType
                                        },
                                        {MessagePropertyConstants.BODY, e.Body},
                                        {MessagePropertyConstants.ROUTING_KEY, e.RoutingKey}
                                    });
                                try
                                {
                                    OnMessageReceived.Invoke(this, new MessageReceivedEventArgs(context));
                                }
                                catch
                                {
                                    // ignored
                                }
                            }
                            return Task.CompletedTask;
                        };

                        consumer.ShutdownAsync += (sender, e) =>
                        {
                            var currentConsumer = ((AsyncEventingBasicConsumer)sender);
                            var currentChannel = currentConsumer.Channel;
                            if (e.ReplyCode == ConnectionForced)
                            {
                                return Task.CompletedTask;
                            }

                            while (e.ReplyCode == ChannelError && !currentChannel.IsOpen)
                            {
                                Thread.Sleep(1000);
                            }

                            return Task.CompletedTask;
                        };

                        _ = await channel.BasicConsumeAsync(GetQueue(topic, queueIndex), false,
                               $"{_amqpConnection.ClientProvidedName}_consumer{queueIndex}",
                               new Dictionary<string, object>(), consumer);
                    }
                    finally
                    {
                        consumers.TryAdd(queueIndex, consumer);
                    }
                }
            }
        }

        private void ConsumeByPull()
        {
            foreach (var topic in _topics.Keys)
            {
                var queueCount = _topics[topic];
                var subscribeQueues = GetSubscribeQueues(queueCount, _consumerCount, _consumerSequence).ToList();
                var subTopic = GetSubTopic(topic);
                var channels = new ConcurrentDictionary<int, (IChannel, Thread)>();
                if (!_globalChannels.TryAdd(subTopic, channels))
                {
                    throw new Exception($"{subTopic} has subscribed.");
                }

                for (int queueIndex = 0; queueIndex < queueCount; queueIndex++)
                {
                    if (!subscribeQueues.Contains(queueIndex))
                    {
                        continue;
                    }

                    var hasValue = channels.TryGetValue(queueIndex, out (IChannel, Thread) channelTuple);
                    if (!hasValue)
                    {
                        continue;
                    }

                    try
                    {
                        var queueName = GetQueue(topic, queueIndex);
                        var channel = _amqpConnection.CreateChannelAsync().Result;
                        var consumerThread = new Thread((state) =>
                            {
                                var currentChannelTopicQueueIndexPair = ((IChannel channel, string topic, int queueIndex))state;
                                var currentChannel = currentChannelTopicQueueIndexPair.channel;
                                var currentTopic = currentChannelTopicQueueIndexPair.topic;
                                var currentQueueIndex = currentChannelTopicQueueIndexPair.queueIndex;
                                var currentQueueName = GetQueue(currentTopic, currentQueueIndex);
                                int unackCount = 0;
                                int noMsgCount = 0;
                                while (true)
                                {
                                    if (!currentChannel.IsOpen)
                                    {
                                        var closeReason = currentChannel.CloseReason;
                                        if (closeReason.ReplyCode == ConnectionForced)
                                        {
                                            break;
                                        }

                                        if (closeReason.ReplyCode == ChannelError)
                                        {
                                            Thread.Sleep(1000);
                                            continue;
                                        }
                                        else
                                        {
                                            throw new Exception($"{closeReason.ReplyText}");
                                        }
                                    }

                                    if (OnMessageReceived == null)
                                    {
                                        Thread.Sleep(1000);
                                        continue;
                                    }

                                    if (unackCount > _prefetchCount)
                                    {
                                        // 当消费堆积数，达到设定值后，将延迟1秒拉消息
                                        Thread.Sleep(1000);
                                    }

                                    try
                                    {
                                        var mqMessage = currentChannel.BasicGetAsync(currentQueueName, false).Result;
                                        if (mqMessage == null)
                                        {
                                            if (noMsgCount < 1000)
                                            {
                                                // 约10秒以内
                                                Interlocked.Increment(ref noMsgCount);
                                                Thread.Sleep(10);
                                            }
                                            else if (noMsgCount < 1500)
                                            {
                                                // 约1分钟以内
                                                Interlocked.Increment(ref noMsgCount);
                                                Thread.Sleep(100);
                                            }
                                            else
                                            {
                                                // 超过1分钟
                                                Thread.Sleep(1000);
                                            }

                                            continue;
                                        }

                                        Interlocked.Exchange(ref noMsgCount, 0);
                                        Interlocked.Increment(ref unackCount);
                                        var context = new MessageHandlingTransportationContext(topic, currentQueueIndex,
                                            _groupName, currentChannel, mqMessage.DeliveryTag,
                                            new Dictionary<string, object>
                                            {
                                                {
                                                    MessagePropertyConstants.MESSAGE_ID,
                                                    mqMessage.BasicProperties.MessageId
                                                },
                                                {MessagePropertyConstants.MESSAGE_TYPE, mqMessage.BasicProperties.Type},
                                                {
                                                    MessagePropertyConstants.TIMESTAMP,
                                                    mqMessage.BasicProperties.Timestamp.UnixTime == 0
                                                        ? DateTime.Now
                                                        : DateTime2UnixTime.FromUnixTime(mqMessage.BasicProperties
                                                            .Timestamp
                                                            .UnixTime)
                                                },
                                                {
                                                    MessagePropertyConstants.CONTENT_TYPE,
                                                    string.IsNullOrEmpty(mqMessage.BasicProperties.ContentType)
                                                        ? "text/json"
                                                        : mqMessage.BasicProperties.ContentType
                                                },
                                                {MessagePropertyConstants.BODY, mqMessage.Body},
                                                {MessagePropertyConstants.ROUTING_KEY, mqMessage.RoutingKey}
                                            });
                                        context.OnAck += (sender, e) => Interlocked.Decrement(ref unackCount);
                                        OnMessageReceived.Invoke(this, new MessageReceivedEventArgs(context));
                                    }
                                    catch
                                    {
                                        // ignored
                                    }
                                }
                            })
                        { IsBackground = false };
                        channelTuple = (channel, consumerThread);
                        consumerThread.Start((channel, topic, queueIndex));
                    }
                    finally
                    {
                        channels.TryAdd(queueIndex, channelTuple);
                    }
                }
            }
        }

        private string GetSubTopic(string topic)
        {
            var groupName = string.IsNullOrEmpty(_groupName) ? "default" : _groupName;
            return $"{topic}.D.{groupName}";
        }

        private string GetQueue(string topic, int queueIndex)
        {
            return $"{GetSubTopic(topic)}-{queueIndex}";
        }

        private IEnumerable<int> GetSubscribeQueues(int queueCount, int consumerCount, int consumerSequence)
        {
            if (consumerSequence > queueCount)
            {
                yield break;
            }

            for (int queueIndex = 0; queueIndex < queueCount; queueIndex++)
            {
                if (queueIndex % consumerCount == consumerSequence - 1)
                {
                    yield return queueIndex;
                }
            }
        }
    }
}
