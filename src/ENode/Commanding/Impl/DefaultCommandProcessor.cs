using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ENode.Configurations;

namespace ENode.Commanding.Impl
{
    public class DefaultCommandProcessor : ICommandProcessor
    {
        private readonly object _lockObj = new object();
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ActivitySource _activitySource;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, ProcessingCommandMailbox> _mailboxDict;
        private readonly IProcessingCommandHandler _handler;
        private readonly IScheduleService _scheduleService;
        private readonly int _timeoutSeconds;
        private readonly string _taskName;

        public DefaultCommandProcessor(IScheduleService scheduleService, IProcessingCommandHandler handler, IJsonSerializer jsonSerializer, ILoggerFactory loggerFactory, ActivitySource activitySource)
        {
            _scheduleService = scheduleService;
            _mailboxDict = new ConcurrentDictionary<string, ProcessingCommandMailbox>();
            _handler = handler;
            _jsonSerializer = jsonSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
            _timeoutSeconds = ENodeConfiguration.Instance.Setting.AggregateRootMaxInactiveSeconds;
            _taskName = "CleanInactiveProcessingCommandMailBoxes_" + DateTime.Now.Ticks + new Random().Next(10000);
            _activitySource = activitySource;
            _logger.InfoFormat("get the activitySource:{0}", activitySource?.Name);
        }

        public void Process(ProcessingCommand processingCommand)
        {
            if (_activitySource != null && processingCommand.Message.Items != null && processingCommand.Message.Items.ContainsKey(nameof(Activity.TraceId)))
            {
                string traceID = processingCommand.Message.Items[nameof(Activity.TraceId)];
                _logger.InfoFormat("start trace:{0}", traceID);
                var actTraceID = ActivityTraceId.CreateFromString(new ReadOnlySpan<char>(traceID.ToCharArray()));
                var actContext = new ActivityContext(actTraceID, ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
                processingCommand.Message.SetActivity(_activitySource.StartActivity(processingCommand.Message.GetType().Name, ActivityKind.Internal, actContext), processingCommand.CommandExecuteContext.QueueMessageID, processingCommand.Message.AggregateRootId);
            }

            var aggregateRootId = processingCommand.Message.AggregateRootId;
            if (string.IsNullOrEmpty(aggregateRootId))
            {
                throw new ArgumentException("aggregateRootId of command cannot be null or empty, commandId:" + processingCommand.Message.Id);
            }

            var mailbox = _mailboxDict.GetOrAdd(aggregateRootId, x =>
            {
                return new ProcessingCommandMailbox(x, _handler, _jsonSerializer, _logger, _activitySource);
            });

            var mailboxTryUsingCount = 0L;
            while (!mailbox.TryUsing())
            {
                Thread.Sleep(1);
                mailboxTryUsingCount++;
                if (mailboxTryUsingCount % 10000 == 0)
                {
                    _logger.WarnFormat("Command mailbox try using count: {0}, aggregateRootId: {1}", mailboxTryUsingCount, mailbox.AggregateRootId);
                }
            }
            if (mailbox.IsRemoved)
            {
                mailbox = new ProcessingCommandMailbox(aggregateRootId, _handler, _jsonSerializer, _logger, _activitySource);
                _mailboxDict.TryAdd(aggregateRootId, mailbox);
            }
            mailbox.EnqueueMessage(processingCommand);
            mailbox.ExitUsing();
        }
        public void Start()
        {
            _scheduleService.StartTask(_taskName, CleanInactiveMailbox, ENodeConfiguration.Instance.Setting.ScanExpiredAggregateIntervalMilliseconds, ENodeConfiguration.Instance.Setting.ScanExpiredAggregateIntervalMilliseconds);
        }
        public void Stop()
        {
            _scheduleService.StopTask(_taskName);
        }

        private void CleanInactiveMailbox()
        {
            var inactiveList = new List<KeyValuePair<string, ProcessingCommandMailbox>>();

            foreach (var pair in _mailboxDict)
            {
                if (IsMailBoxAllowRemove(pair.Value))
                {
                    inactiveList.Add(pair);
                }
            }

            foreach (var pair in inactiveList)
            {
                var mailbox = pair.Value;
                if (mailbox.TryUsing())
                {
                    if (IsMailBoxAllowRemove(mailbox))
                    {
                        if (_mailboxDict.TryRemove(pair.Key, out ProcessingCommandMailbox removed))
                        {
                            removed.MarkAsRemoved();
                            _logger.InfoFormat("Removed inactive command mailbox, aggregateRootId: {0}", pair.Key);
                        }
                    }
                }
                mailbox.ExitUsing();
            }
        }
        private bool IsMailBoxAllowRemove(ProcessingCommandMailbox mailbox)
        {
            return mailbox.IsInactive(_timeoutSeconds) && !mailbox.IsRunning && mailbox.TotalUnHandledMessageCount == 0;
        }
    }
}
