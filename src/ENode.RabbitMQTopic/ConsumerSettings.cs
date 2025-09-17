using RabbitMQ.Client;
using System;
using IRabbitMQConnection = RabbitMQ.Client.IConnection;

namespace RabbitMQTopic
{
    /// <summary>
    /// 消费者设置
    /// </summary>
    public class ConsumerSettings
    {
        /// <summary>
        /// 构造函数，用于初始化rabbitmq的连接
        /// </summary>
        public ConsumerSettings(string hostName, int port, string userName, string password, string clientName)
        {
            // var hostName = NacosConfigHelper.Instance.GetValue("RabbitMQHost", "");
            // var port = NacosConfigHelper.Instance.GetValue("RabbitMQPort", 5672);
            // var userName = NacosConfigHelper.Instance.GetValue("RabbitMQUserName", "");
            // var password = NacosConfigHelper.Instance.GetValue("RabbitMQPassword", "");
            var factory = new ConnectionFactory
            {
                //设置主机名
                HostName = hostName,

                //设置心跳时间
                //RequestedHeartbeat = TimeSpan.FromMilliseconds(config.HeartBeat),

                //设置自动重连
                AutomaticRecoveryEnabled = true,

                //重连时间
                //NetworkRecoveryInterval = config.NetworkRecoveryInterval,

                //用户名
                UserName = userName,

                //密码
                Password = password,
                //端口
                Port = port,
            };
            if (!string.IsNullOrEmpty(hostName)) AmqpConnection = factory.CreateConnectionAsync().Result;
            if (!string.IsNullOrWhiteSpace(clientName)) ClientName = clientName;
        }
        /// <summary>
        /// 客户端名
        /// </summary>
        public string ClientName { get; set; }

        /// <summary>
        /// AMQP Uri（AmqpUri、AmqpConnection，至少设置一个）
        /// </summary>
        public Uri AmqpUri { get; set; }

        /// <summary>
        /// AMQP连接（AmqpUri、AmqpConnection，至少设置一个）
        /// </summary>
        public IRabbitMQConnection AmqpConnection { get; set; }

        /// <summary>
        /// 消费者组名
        /// </summary>
        public string GroupName { get; set; }

        /// <summary>
        /// 消费者个数（用于负载自动分配队列）
        /// </summary>
        public int ConsumerCount { get; set; }

        /// <summary>
        /// 消费者序号，从1开始（用于负载自动分配队列）
        /// </summary>
        public int ConsumerSequence { get; set; }

        /// <summary>
        /// 消费模式（默认：Push）
        /// </summary>
        public ConsumeMode Mode { get; set; }

        /// <summary>
        /// 每个队列的预抓取消息数（Pull模式下，未响应数超过此设置后，将暂停1秒后拉取消息）
        /// </summary>
        public int PrefetchCount { get; set; }
    }
}
