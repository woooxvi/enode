using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Infrastructure;
using RabbitMQ.Client;
using RabbitMQTopic;

namespace ENode.RabbitMQ
{
    internal class SendReplyService
    {
        private readonly string _name;
        // private readonly ConcurrentDictionary<string, SocketRemotingClientWrapper> _remotingClientDict;
        private readonly IJsonSerializer _jsonSerializer;
        private ITypeNameProvider _typeNameProvider;
        // private readonly IScheduleService _scheduleService;
        private readonly IOHelper _ioHelper;
        private readonly ILogger _logger;
        private readonly string _scanInactiveRemotingClientTaskName;
        private ConsumerSettings _settings;
        private ConcurrentQueue<SendReplyContext> _sendReplyQueue;

        private int _sendMsgTimeout = 3000;

        private IChannel _replySender;
        private volatile int _isRunning = 0;
        private readonly Timer _sendReplyTimer;
        private readonly int _sendInterval = 1;


        public SendReplyService(string name, ConsumerSettings settings)
        {
            _name = name;
            // _remotingClientDict = new ConcurrentDictionary<string, SocketRemotingClientWrapper>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            // _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scanInactiveRemotingClientTaskName = name + "_ScanInactiveRemotingClient_" + DateTime.Now.Ticks + new Random().Next(10000);
            _settings = settings;
            _sendReplyQueue = new ConcurrentQueue<SendReplyContext>();
            _sendReplyTimer = new Timer(SendReplyCore);
            _logger.InfoFormat("Created, name: {0}, settings: {1}", name, settings.ToString());
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 1, 0) == 0)
            {
                // _scheduleService.StartTask(_scanInactiveRemotingClientTaskName, ScanInactiveRemotingClients, 5000, 5000);
                _replySender = _settings.AmqpConnection.CreateChannelAsync().Result;
                _sendReplyTimer.Change(TimeSpan.FromSeconds(_sendInterval), TimeSpan.FromSeconds(_sendInterval));
                _logger.InfoFormat("Started, name: {0}", _name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="replyType"></param>
        /// <param name="replyData"><see cref="CommandResult"/> or <see cref="DomainEventHandledMessage"/></param>
        /// <param name="replyAddress"></param>
        /// <param name="commandId"></param>
        public void SendReply(short replyType, object replyData, string replyAddress, string commandId)
        {
            var context = new SendReplyContext(replyType, replyData, replyAddress);
            _sendReplyQueue.Enqueue(context);
            _logger.InfoFormat("Enqueued reply message, replyAddress: {0}, CommandId: {1}", replyAddress, commandId);
            // Task.Factory.StartNew(obj =>
            // {
            //     var context = obj as SendReplyContext;
            //     SocketRemotingClientWrapper remotingClientWrapper = null;
            //     _logger.Info("Send replying, replyAddress: " + context.ReplyAddress);
            //     var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity(_name + ".SendReply");
            //     act?.SetTag(replyAddress, 1);
            //     try
            //     {
            //         var message = _jsonSerializer.Serialize(context.ReplyData);
            //         var body = Encoding.UTF8.GetBytes(message);
            //         var request = new RedisRemotingRequest(context.ReplyType, body);
            //         request.TraceId = act?.TraceId.ToString();
            //         //remotingClientWrapper = GetRemotingClient(context.ReplyAddress);
            //         lock (this)
            //         {
            //             //remotingClientWrapper.SocketRemotingClient.Start();
            //             //remotingClientWrapper.SocketRemotingClient.InvokeOneway(request);
            //             //remotingClientWrapper.LastSendMessageTime = DateTime.Now;

            //             var json = Newtonsoft.Json.JsonConvert.SerializeObject(request);
            //             var _message = new RabbitMQTopic.Message(
            //                 replyAddress,
            //                 (int)MessageTypeCode.ApplicationMessage,
            //                 Encoding.UTF8.GetBytes(json),
            //                 "text/json",
            //                 _typeNameProvider.GetTypeName(message.GetType()));
            //             var sendRes = _replySender.SendMessage(_message, replyAddress);
            //             if (sendRes.SendStatus != SendStatus.Success)
            //             {
            //                 _logger.Error($"Send reply message failed, replyAddress:{replyAddress},ErrorMessage:{sendRes.ErrorMessage}");
            //             }
            //             // var channel = StackExchange.Redis.RedisChannel.Literal(ENodeExtensions.ReplySeviceMQTopic + replyAddress);
            //             // RedisHelper.GetInstance(connString: _redisConnection).Publish(channel, Newtonsoft.Json.JsonConvert.SerializeObject(request));
            //             act?.SetTag("request.Id", request.Id);
            //             act?.SetTag("message", message);
            //             act?.SetTag("CommandId", commandId);
            //         }
            //     }
            //     catch (Exception ex)
            //     {
            //         act?.SetTag("Exception", ex.Message);
            //         _logger.Error("Send reply has exeption, replyAddress: " + context.ReplyAddress, ex);
            //         remotingClientWrapper.SocketRemotingClient.Shutdown();
            //     }
            //     act?.Stop();
            // }, new SendReplyContext(replyType, replyData, replyAddress));
            // return Task.CompletedTask;
        }

        private void SendReplyCore(object state)
        {
            _sendReplyTimer.Change(Timeout.Infinite, Timeout.Infinite);
            try
            {
                while (_sendReplyQueue.TryDequeue(out SendReplyContext context) && _isRunning == 1)
                {
                    _logger.InfoFormat("Send replying, replyAddress: {0}", context.ReplyAddress);
                    var body = Encoding.UTF8.GetBytes(_jsonSerializer.Serialize(context.ReplyData));
                    var request = new RedisRemotingRequest(context.ReplyType, body);
                    body = Encoding.UTF8.GetBytes(_jsonSerializer.Serialize(request));
                    
                    var properties = new BasicProperties();
                    properties.Persistent = true;
                    // properties.ContentType = MimeTypes.Json;
                    properties.MessageId = Guid.NewGuid().ToString();
                    // properties.Type = message.Tag ?? string.Empty;
                    // properties.Timestamp = new AmqpTimestamp(DateTime2UnixTime.ToUnixTime(createdTime));
                    //if (_delayedMessageEnabled && message.DelayedMilliseconds > 0)
                    //{
                    //    properties.Headers = new Dictionary<string, object>
                    //    {
                    //        {"x-delay", message.DelayedMilliseconds}
                    //    };
                    //}

                    try
                    {
                        CancellationTokenSource cancellation = new(_sendMsgTimeout);
                        _replySender.BasicPublishAsync(
                            exchange: ENodeExtensions.ReplyServiceMQExchange,
                            routingKey: context.ReplyAddress,
                            mandatory: true,
                            basicProperties: properties,
                            body: body,
                            cancellationToken: cancellation.Token).GetAwaiter();
                        _logger.InfoFormat("Sent reply message, replyAddress: {0}", context.ReplyAddress);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error($"Send reply message failed, replyAddress:{context.ReplyAddress},Exception:{ex.Message}", ex);
                        Console.Error.WriteLine($"Wait for confirms failed({ex.Message}).");
                    }
                }
            }
            finally
            {
                if (_isRunning == 1)
                {
                    _sendReplyTimer.Change(TimeSpan.FromSeconds(_sendInterval), TimeSpan.FromSeconds(_sendInterval));
                }
            }
        }

        public void Stop()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 0, 1) == 1)
            {
                _logger.InfoFormat("Stopping, name: {0}", _name);
                _replySender.CloseAsync().Wait();
                _replySender.Dispose();
            }
            // _scheduleService.StopTask(_scanInactiveRemotingClientTaskName);
            // foreach (var remotingClient in _remotingClientDict.Values)
            // {
            //     remotingClient.SocketRemotingClient.Shutdown();
            // }
        }

        // private void ScanInactiveRemotingClients()
        // {
        //     lock (this)
        //     {
        //         var inactiveList = new List<KeyValuePair<string, SocketRemotingClientWrapper>>();
        //         foreach (var pair in _remotingClientDict)
        //         {
        //             if (!pair.Value.SocketRemotingClient.IsConnected || (DateTime.Now - pair.Value.LastSendMessageTime).TotalSeconds > 300)
        //             {
        //                 inactiveList.Add(pair);
        //             }
        //         }
        //         foreach (var pair in inactiveList)
        //         {
        //             if (_remotingClientDict.TryRemove(pair.Key, out SocketRemotingClientWrapper removed))
        //             {
        //                 removed.SocketRemotingClient.Shutdown();
        //                 _logger.InfoFormat("Removed disconnected remoting client, remotingAddress: {0}", pair.Key);
        //             }
        //         }
        //     }
        // }
        // private SocketRemotingClientWrapper GetRemotingClient(string replyAddress)
        // {
        //     if (_remotingClientDict.TryGetValue(replyAddress, out SocketRemotingClientWrapper remotingClientWrapper))
        //     {
        //         if (remotingClientWrapper.SocketRemotingClient.IsConnected)
        //         {
        //             return remotingClientWrapper;
        //         }
        //         else
        //         {
        //             _remotingClientDict.TryRemove(replyAddress, out SocketRemotingClientWrapper removed);
        //         }
        //     }

        //     return CreateReplyRemotingClient(replyAddress);
        // }
        // private SocketRemotingClientWrapper CreateReplyRemotingClient(string replyAddress)
        // {
        //     return _remotingClientDict.GetOrAdd(replyAddress, key =>
        //     {
        //         return new SocketRemotingClientWrapper
        //         {
        //             SocketRemotingClient = new SocketRemotingClient(_name, TryParseReplyAddress(replyAddress)),
        //             LastSendMessageTime = DateTime.Now
        //         };
        //     });
        // }

        private IPEndPoint TryParseReplyAddress(string replyAddress)
        {
            try
            {
                var items = replyAddress.Split(':');
                Ensure.Equals(items.Length, 2);
                return new IPEndPoint(IPAddress.Parse(items[0]), int.Parse(items[1]));
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Invalid reply address : {0}", replyAddress), ex);
                return null;
            }
        }

        class SendReplyContext
        {
            public short ReplyType { get; private set; }
            /// <summary>
            /// <see cref="CommandResult"/> or <see cref="DomainEventHandledMessage"/>
            /// </summary>
            public object ReplyData { get; private set; }
            public string ReplyAddress { get; private set; }

            public SendReplyContext(short replyType, object replyData, string replyAddress)
            {
                ReplyType = replyType;
                ReplyData = replyData;
                ReplyAddress = replyAddress;
            }
        }
    }
}
