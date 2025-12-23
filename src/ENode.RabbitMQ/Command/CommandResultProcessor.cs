using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ENode.Commanding;
using Microsoft.Extensions.Caching.Memory;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQTopic;

namespace ENode.RabbitMQ
{
    /// <summary>
    /// Command Result Processor
    /// </summary>
    public class CommandResultProcessor
    {
        // private SocketRemotingServer _remotingServer;
        private ConcurrentDictionary<string, CommandTaskCompletionSource> _commandTaskDict;
        private BlockingCollection<CommandResult> _commandExecutedMessageLocalQueue;
        private BlockingCollection<DomainEventHandledMessage> _domainEventHandledMessageLocalQueue;
        private Worker _commandExecutedMessageWorker;
        private Worker _domainEventHandledMessageWorker;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private bool _started;
        private MemoryCache _cache;
        private MemoryCacheEntryOptions _cacheOption;
        // private Consumer _producerReplyReceive;

        public string ClientName => _settings?.AmqpConnection?.ClientProvidedName;

        private ProducerSettings _settings;
        private IChannel _replyReceiver;
        // public string ClientName => _producerReplyReceive?.ClientName;

        private const ushort ChannelError = 504;
        private const ushort ConnectionForced = 320;

        /// <summary>
        /// BindingAddress for receive command result reply.
        /// </summary>
        // public IPEndPoint BindingAddress { get; private set; }
        // public IPEndPoint ReplyAddress { get; private set; }


        /// <summary>
        /// 初始化Command结果处理器
        /// </summary>
        /// <param name="settings">生产者的设置，会使用其中的rabbitmq-client</param>
        /// <returns></returns>
        public CommandResultProcessor Initialize(ProducerSettings settings)
        {
            // _remotingServer = new SocketRemotingServer("CommandResultProcessor.RemotingServer", bindingAddress);
            _commandTaskDict = new ConcurrentDictionary<string, CommandTaskCompletionSource>();
            _commandExecutedMessageLocalQueue = new BlockingCollection<CommandResult>(new ConcurrentQueue<CommandResult>());
            _commandExecutedMessageWorker = new Worker("ProcessExecutedCommandMessage", () => ProcessExecutedCommandMessage(_commandExecutedMessageLocalQueue.Take()));
            _domainEventHandledMessageLocalQueue = new BlockingCollection<DomainEventHandledMessage>(new ConcurrentQueue<DomainEventHandledMessage>());
            _domainEventHandledMessageWorker = new Worker("ProcessDomainEventHandledMessage", () => ProcessDomainEventHandledMessage(_domainEventHandledMessageLocalQueue.Take()));
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _settings = settings;
            // _producerReplyReceive = new Consumer(settings, true);
            // BindingAddress = bindingAddress;
            // ReplyAddress = replyAddress;

            _cache = new MemoryCache(new MemoryCacheOptions());
            _cacheOption = new MemoryCacheEntryOptions() { AbsoluteExpiration = DateTime.Now.AddSeconds(30) };
            _cacheOption.RegisterPostEvictionCallback((k, v, r, o) =>
            {
                if (r == EvictionReason.Removed)
                    _logger.Fatal("delete from memorycache.key:" + k);
            });
            return this;
        }

        /// <summary>
        /// Register Processing Command
        /// </summary>
        /// <param name="command"></param>
        /// <param name="commandReturnType"></param>
        /// <param name="taskCompletionSource"></param>
        public void RegisterProcessingCommand(ICommand command, CommandReturnType commandReturnType, TaskCompletionSource<CommandResult> taskCompletionSource)
        {
            var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity("CommandResultProcessor.RegisterProcessingCommand");
            act?.SetTag("AggregateRootId", command.AggregateRootId);
            act?.SetTag("CommandId", command.Id);
            var task = new CommandTaskCompletionSource { CommandReturnType = commandReturnType, TaskCompletionSource = taskCompletionSource };
            if (!_commandTaskDict.TryAdd(command.Id, task))
            {
                throw new Exception(string.Format("Duplicate processing command registration, type:{0}, id:{1}", command.GetType().Name, command.Id));
            }
            _cache.Set(command.Id, task, _cacheOption);
            act?.SetTag("CommandTaskCompletionSource", _commandTaskDict[command.Id]?.GetHashCode());
            act?.Stop();
        }

        /// <summary>
        /// Process Failed Sending Command
        /// </summary>
        /// <param name="command"></param>
        public void ProcessFailedSendingCommand(ICommand command)
        {
            if (_commandTaskDict.TryRemove(command.Id, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                var commandResult = new CommandResult(CommandStatus.Failed, command.Id, command.AggregateRootId, "Failed to send the command.", typeof(string).FullName);
                _logger.Fatal("CommandTaskCompletionSource removed in ProcessFailedSendingCommand. res:" + commandResult);
                commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult);
            }
        }

        /// <summary>
        /// Start
        /// </summary>
        /// <returns></returns>
        public CommandResultProcessor Start()
        {
            if (_started) return this;

            // _remotingServer.Start();
            _commandExecutedMessageWorker.Start();
            _domainEventHandledMessageWorker.Start();

            // _remotingServer.RegisterRequestHandler((int)CommandReturnType.CommandExecuted, this);
            // _remotingServer.RegisterRequestHandler((int)CommandReturnType.EventHandled, this);
            _replyReceiver = _settings.AmqpConnection.CreateChannelAsync().Result;
            _replyReceiver.ExchangeDeclareAsync(ENodeExtensions.ReplyServiceMQExchange, ExchangeType.Direct, true, false, null).Wait();


            // 设置队列参数：30分钟(1800000毫秒)无消费者则自动删除
            var queueArguments = new Dictionary<string, object>
            {
                { "x-expires", 10 * 60 * 1000 },
                // 可选：如果希望队列在最后一个消费者断开连接后立即删除，可添加此参数
                // { "auto_delete", true }
            };

            _replyReceiver.QueueDeclareAsync(this.ClientName, true, false, false, queueArguments).Wait();
            _replyReceiver.QueueBindAsync(this.ClientName, ENodeExtensions.ReplyServiceMQExchange, this.ClientName, null).Wait();

            _replyReceiver.BasicQosAsync(0, 1, false).Wait();
            var consumer = new AsyncEventingBasicConsumer(_replyReceiver);

            try
            {
                consumer.ReceivedAsync += (sender, e) =>
                {
                    var currentConsumer = ((AsyncEventingBasicConsumer)sender);
                    var currentChannel = currentConsumer.Channel;

                    var currentTopic = e.Exchange.IndexOf("-delayed", StringComparison.Ordinal) > 0
                        ? e.Exchange.Substring(0,
                            e.Exchange.LastIndexOf("-delayed", StringComparison.Ordinal))
                        : e.Exchange;
                    var request = _jsonSerializer.Deserialize<RedisRemotingRequest>(Encoding.UTF8.GetString(e.Body.ToArray()));
                    InternalHandleRequest(request);
                    currentChannel.BasicAckAsync(e.DeliveryTag, false);
                    // var currentQueueIndex = consumers.First(w => w.Value.Channel == currentChannel).Key;
                    // var context = new MessageHandlingTransportationContext(currentTopic, currentQueueIndex,
                    //     _groupName, _replyReceiver, e.DeliveryTag, new Dictionary<string, object>
                    //     {
                    //         {MessagePropertyConstants.MESSAGE_ID, e.BasicProperties.MessageId},
                    //         {MessagePropertyConstants.MESSAGE_TYPE, e.BasicProperties.Type},
                    //         {
                    //             MessagePropertyConstants.TIMESTAMP,
                    //             e.BasicProperties.Timestamp.UnixTime == 0
                    //                 ? DateTime.Now
                    //                 : DateTime2UnixTime.FromUnixTime(e.BasicProperties.Timestamp.UnixTime)
                    //         },
                    //         {
                    //             MessagePropertyConstants.CONTENT_TYPE,
                    //             string.IsNullOrEmpty(e.BasicProperties.ContentType)
                    //                 ? "text/json"
                    //                 : e.BasicProperties.ContentType
                    //         },
                    //         {MessagePropertyConstants.BODY, e.Body},
                    //         {MessagePropertyConstants.ROUTING_KEY, e.RoutingKey}
                    //     });
                    // try
                    // {
                    //     OnMessageReceived.Invoke(this, new MessageReceivedEventArgs(context));
                    // }
                    // catch
                    // {
                    //     // ignored
                    // }
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

                _replyReceiver.BasicConsumeAsync(this.ClientName, false,
                        $"{this.ClientName}_consumer",
                        new Dictionary<string, object>(), consumer).Wait();
            }
            finally
            {
                // consumers.TryAdd(queueIndex, consumer);
            }
            _started = true;
            return this;
        }

        /// <summary>
        /// Shutdown
        /// </summary>
        /// <returns></returns>
        public CommandResultProcessor Shutdown()
        {
            // _remotingServer.Shutdown();
            _replyReceiver.CloseAsync().Wait();
            _commandExecutedMessageWorker.Stop();
            _domainEventHandledMessageWorker.Stop();
            return this;
        }


        private RemotingResponse InternalHandleRequest(RemotingRequest remotingRequest)
        {
            string traceId = (remotingRequest as RedisRemotingRequest)?.TraceId;
            ActivityContext context = new ActivityContext(traceId?.Length == 32 ? ActivityTraceId.CreateFromString(new ReadOnlySpan<char>(traceId?.ToCharArray())) : ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
            var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity("CommandResultProcessor.HandleRequest", ActivityKind.Internal, context);
            string commandId = "";
            if (remotingRequest.Code == (int)CommandReturnType.CommandExecuted)
            {
                var json = Encoding.UTF8.GetString(remotingRequest.Body);
                var result = _jsonSerializer.Deserialize<MyCommandResult>(json);
                result.TraceId = traceId;
                act?.SetTag("AggregateRootId", result.AggregateRootId);
                act?.SetTag("CommandExecuted", json);
                commandId = result.CommandId;
                _commandExecutedMessageLocalQueue.Add(result);
            }
            else if (remotingRequest.Code == (int)CommandReturnType.EventHandled)
            {
                var json = Encoding.UTF8.GetString(remotingRequest.Body);
                act?.SetTag("EventHandled", json);
                var message = _jsonSerializer.Deserialize<DomainEventHandledMessage>(json);
                act?.SetTag("AggregateRootId", message.AggregateRootId);
                commandId = message.CommandId;
                _domainEventHandledMessageLocalQueue.Add(message);
            }
            else
            {
                act?.SetTag("Invalid remoting", remotingRequest.Code);
                _logger.ErrorFormat("Invalid remoting request code: {0}", remotingRequest.Code);
            }
            act?.SetTag("CheckTaskSource", _commandTaskDict.ContainsKey(commandId));
            act?.SetTag("CommandId", commandId);
            act?.Stop();
            return null;
        }

        private void ProcessExecutedCommandMessage(CommandResult commandResult)
        {
            string traceId = (commandResult as MyCommandResult)?.TraceId;
            ActivityContext context = new ActivityContext(traceId?.Length == 32 ? ActivityTraceId.CreateFromString(new ReadOnlySpan<char>(traceId?.ToCharArray())) : ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
            var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity("CommandResultProcessor.ProcessExecutedCommandMessage", ActivityKind.Internal, context);
            act?.SetTag("CommandId", commandResult.CommandId);
            act?.SetTag("AggregateRootId", commandResult.AggregateRootId);
            if (_commandTaskDict.TryGetValue(commandResult.CommandId, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                if (commandTaskCompletionSource.CommandReturnType == CommandReturnType.CommandExecuted)
                {
                    act?.SetTag("TrySetResult", commandResult);
                    if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                    {
                        _commandTaskDict.Remove(commandResult.CommandId);
                        //_logger.Fatal("CommandTaskCompletionSource removed in ProcessExecutedCommandMessage_Command. res:" + commandResult);
                        act?.SetTag("SetSucc", 1);
                        if (_logger.IsDebugEnabled)
                        {
                            _logger.DebugFormat("Command result return, {0}", commandResult);
                        }
                    }
                }
                else if (commandTaskCompletionSource.CommandReturnType == CommandReturnType.EventHandled)
                {
                    if (commandResult.Status == CommandStatus.Failed || commandResult.Status == CommandStatus.NothingChanged)
                    {
                        if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                        {
                            _commandTaskDict.Remove(commandResult.CommandId);
                            _logger.Fatal("CommandTaskCompletionSource removed in ProcessExecutedCommandMessage_Event. res:" + commandResult);
                            if (_logger.IsDebugEnabled)
                            {
                                _logger.DebugFormat("Command result return, {0}", commandResult);
                            }
                        }
                    }
                }
            }
            else
            {
                var task = _cache.Get<CommandTaskCompletionSource>(commandResult.CommandId);
                if (task != null)
                {
                    act?.SetTag("InCache", true);
                    if (task.TaskCompletionSource.TrySetResult(commandResult))
                        _cache.Remove(commandResult.CommandId);
                    else
                        _logger.Fatal($"commandId:{commandResult.CommandId} TaskCompletionSource.TrySetResult:false");
                }
                else
                {
                    act?.SetTag("commandTaskDictKeys", string.Join(',', _commandTaskDict.Keys));
                    _logger.Fatal($"traceId:{traceId} commandTaskDictKeys:" + string.Join(',', _commandTaskDict.Keys));
                }
            }
            act?.Stop();
        }
        private void ProcessDomainEventHandledMessage(DomainEventHandledMessage message)
        {
            if (_commandTaskDict.TryGetValue(message.CommandId, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                var commandResult = new CommandResult(CommandStatus.Success, message.CommandId, message.AggregateRootId, message.CommandResult, message.CommandResult != null ? typeof(string).FullName : null);
                if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                {
                    _logger.Fatal("CommandTaskCompletionSource removed in ProcessDomainEventHandledMessage. res:" + commandResult);
                    _commandTaskDict.Remove(message.CommandId);
                    if (_logger.IsDebugEnabled)
                    {
                        _logger.DebugFormat("Command result return, {0}", commandResult);
                    }
                }
            }
        }

        class CommandTaskCompletionSource
        {
            public TaskCompletionSource<CommandResult> TaskCompletionSource { get; set; }
            public CommandReturnType CommandReturnType { get; set; }
        }
    }
}
