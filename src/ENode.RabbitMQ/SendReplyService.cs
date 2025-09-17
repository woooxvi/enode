using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;

namespace ENode.RabbitMQ
{
    internal class SendReplyService
    {
        private readonly string _name;
        private readonly string _redisConnection;
        private readonly ConcurrentDictionary<string, SocketRemotingClientWrapper> _remotingClientDict;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IScheduleService _scheduleService;
        private readonly IOHelper _ioHelper;
        private readonly ILogger _logger;
        private readonly string _scanInactiveRemotingClientTaskName;

        public SendReplyService(string name, string redisCoonnection)
        {
            _name = name;
            _redisConnection = redisCoonnection;
            _remotingClientDict = new ConcurrentDictionary<string, SocketRemotingClientWrapper>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scanInactiveRemotingClientTaskName = name + "_ScanInactiveRemotingClient_" + DateTime.Now.Ticks + new Random().Next(10000);
        }

        public void Start()
        {
            _scheduleService.StartTask(_scanInactiveRemotingClientTaskName, ScanInactiveRemotingClients, 5000, 5000);
        }
        public void Stop()
        {
            _scheduleService.StopTask(_scanInactiveRemotingClientTaskName);
            foreach (var remotingClient in _remotingClientDict.Values)
            {
                remotingClient.SocketRemotingClient.Shutdown();
            }
        }
        public Task SendReply(short replyType, object replyData, string replyAddress, string commandId)
        {
            Task.Factory.StartNew(obj =>
            {
                var context = obj as SendReplyContext;
                SocketRemotingClientWrapper remotingClientWrapper = null;
                _logger.Info("Send replying, replyAddress: " + context.ReplyAddress);
                var act = ObjectContainer.Resolve<ActivitySource>()?.StartActivity(_name + ".SendReply");
                act?.SetTag(replyAddress, 1);
                try
                {
                    var message = _jsonSerializer.Serialize(context.ReplyData);
                    var body = Encoding.UTF8.GetBytes(message);
                    var request = new RedisRemotingRequest(context.ReplyType, body);
                    request.TraceId = act?.TraceId.ToString();
                    //remotingClientWrapper = GetRemotingClient(context.ReplyAddress);
                    lock (this)
                    {
                        //remotingClientWrapper.SocketRemotingClient.Start();
                        //remotingClientWrapper.SocketRemotingClient.InvokeOneway(request);
                        //remotingClientWrapper.LastSendMessageTime = DateTime.Now;
                        var channel = StackExchange.Redis.RedisChannel.Literal(ENodeExtensions.ReplySeviceMQTopic + replyAddress);
                        RedisHelper.GetInstance(connString: _redisConnection).Publish(channel, Newtonsoft.Json.JsonConvert.SerializeObject(request));
                        act?.SetTag("request.Id", request.Id);
                        act?.SetTag("message", message);
                        act?.SetTag("CommandId", commandId);
                    }
                }
                catch (Exception ex)
                {
                    act?.SetTag("Exception", ex.Message);
                    _logger.Error("Send reply has exeption, replyAddress: " + context.ReplyAddress, ex);
                    remotingClientWrapper.SocketRemotingClient.Shutdown();
                }
                act?.Stop();
            }, new SendReplyContext(replyType, replyData, replyAddress));
            return Task.CompletedTask;
        }

        private void ScanInactiveRemotingClients()
        {
            lock (this)
            {
                var inactiveList = new List<KeyValuePair<string, SocketRemotingClientWrapper>>();
                foreach (var pair in _remotingClientDict)
                {
                    if (!pair.Value.SocketRemotingClient.IsConnected || (DateTime.Now - pair.Value.LastSendMessageTime).TotalSeconds > 300)
                    {
                        inactiveList.Add(pair);
                    }
                }
                foreach (var pair in inactiveList)
                {
                    if (_remotingClientDict.TryRemove(pair.Key, out SocketRemotingClientWrapper removed))
                    {
                        removed.SocketRemotingClient.Shutdown();
                        _logger.InfoFormat("Removed disconnected remoting client, remotingAddress: {0}", pair.Key);
                    }
                }
            }
        }
        private SocketRemotingClientWrapper GetRemotingClient(string replyAddress)
        {
            if (_remotingClientDict.TryGetValue(replyAddress, out SocketRemotingClientWrapper remotingClientWrapper))
            {
                if (remotingClientWrapper.SocketRemotingClient.IsConnected)
                {
                    return remotingClientWrapper;
                }
                else
                {
                    _remotingClientDict.TryRemove(replyAddress, out SocketRemotingClientWrapper removed);
                }
            }

            return CreateReplyRemotingClient(replyAddress);
        }
        private SocketRemotingClientWrapper CreateReplyRemotingClient(string replyAddress)
        {
            return _remotingClientDict.GetOrAdd(replyAddress, key =>
            {
                return new SocketRemotingClientWrapper
                {
                    SocketRemotingClient = new SocketRemotingClient(_name, TryParseReplyAddress(replyAddress)),
                    LastSendMessageTime = DateTime.Now
                };
            });
        }

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
            public object ReplyData { get; private set; }
            public string ReplyAddress { get; private set; }

            public SendReplyContext(short replyType, object replyData, string replyAddress)
            {
                ReplyType = replyType;
                ReplyData = replyData;
                ReplyAddress = replyAddress;
            }
        }
        class SocketRemotingClientWrapper
        {
            public SocketRemotingClient SocketRemotingClient { get; set; }
            public DateTime LastSendMessageTime { get; set; }
        }
    }
}
