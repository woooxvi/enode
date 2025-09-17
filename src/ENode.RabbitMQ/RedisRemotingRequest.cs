using ECommon.Remoting;
using ENode.Commanding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ENode.RabbitMQ
{
    public class RedisRemotingRequest : RemotingRequest
    {
        public RedisRemotingRequest(short code, byte[] body) : base(code, body)
        {

        }
        public string TraceId { get; set; }
    }

    public class MyCommandResult:CommandResult
    {
        public string TraceId { get; set; }
    }
}
