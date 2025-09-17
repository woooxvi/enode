using RabbitMQ.Client;
using System;
using IRabbitMQConnection = RabbitMQ.Client.IConnection;

namespace RabbitMQTopic
{
    /// <summary>
    /// 生产者配置
    /// </summary>
    public class ProducerSettings
    {
        /// <summary>
        /// 构造函数，提供连接rabbitmq的必要参数
        /// </summary>
        public ProducerSettings(string hostName, int port, string userName, string password, string clientName)
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
        /// 发送消息超时时间（默认：3s）
        /// </summary>
        public int SendMsgTimeout { get; set; }

        /// <summary>
        /// 最大Channel空闲时长（默认：10s）
        /// </summary>
        public long MaxChannelIdleDuration { get; set; }

        /// <summary>
        /// 最大Channel池大小（默认：1000）
        /// </summary>
        public int MaxChannelPoolSize { get; set; }
    }
}
