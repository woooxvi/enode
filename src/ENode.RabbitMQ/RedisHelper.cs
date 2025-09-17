using System.Collections.Concurrent;
using StackExchange.Redis;
public class RedisHelper
{

    #region 构造函数

    public const string DEF_REDIS_CONNKEY = "RedisConnection";
    private IConnectionMultiplexer _connMultiplexer;
    private int db = 0;
    /// <summary>
    /// 数据库
    /// </summary>
    private IDatabase _db;

    private RedisHelper() { }

    private RedisHelper(int db = -1, string connString = DEF_REDIS_CONNKEY)
    {
        _connMultiplexer = ConnectionMultiplexer.Connect(connString);
        //DefaultKey = NacosConfigHelper.Instance.GetValue("Redis.DefaultKey");
        // AddRegisterEvent();

        this.db = db;
        _db = _connMultiplexer.GetDatabase(db);
        Console.WriteLine("redisHepler db:{0} created.", db);
    }

    private static object instanceLock = new object();
    private static ConcurrentDictionary<string, Lazy<RedisHelper>> instances = new ConcurrentDictionary<string, Lazy<RedisHelper>>();

    /// <summary>
    /// 2023年8月30日 业务redis存放情况<para />
    /// 8: APP-API的L2缓存，以及变更通知<para />
    /// 9: <para />
    /// 10: L3缓存等<para />
    /// 11: enode快照<para />
    /// 12: currentUser缓存<para />
    /// </summary>
    /// <param name="db"></param>
    /// <param name="connString"></param>
    /// <returns></returns>
    public static RedisHelper GetInstance(int db = 10, string connString = DEF_REDIS_CONNKEY)
    {
        var key = $"{db},{connString}";
        var instance = instances.GetOrAdd(key, (k) =>
            new Lazy<RedisHelper>(() =>
            {
                return new RedisHelper(db, connString);
            }, LazyThreadSafetyMode.ExecutionAndPublication)
        );
        return instance.Value;
    }

    //public RedisHelper(int db = -1)
    //{
    //    this.db = db;
    //    _db = _connMultiplexer.GetDatabase(db);
    //}


    #endregion 构造函数


    #region 发布订阅

    /// <summary>
    /// 订阅
    /// </summary>
    /// <param name="channel"></param>
    /// <param name="handle"></param>
    public void Subscribe(RedisChannel channel, Action<RedisChannel, RedisValue> handle)
    {
        var sub = _connMultiplexer.GetSubscriber();
        sub.Subscribe(channel, handle);
    }

    /// <summary>
    /// 发布
    /// </summary>
    /// <param name="channel"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    public long Publish(RedisChannel channel, RedisValue message)
    {
        var sub = _connMultiplexer.GetSubscriber();
        return sub.Publish(channel, message);
    }

    /// <summary>
    /// 发布（使用序列化）
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="channel"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    public long Publish<T>(RedisChannel channel, T message)
    {
        var sub = _connMultiplexer.GetSubscriber();
        return sub.Publish(channel, Serialize(message));
    }

    #region 发布订阅-async

    /// <summary>
    /// 订阅
    /// </summary>
    /// <param name="channel"></param>
    /// <param name="handle"></param>
    public async Task SubscribeAsync(RedisChannel channel, Action<RedisChannel, RedisValue> handle)
    {
        var sub = _connMultiplexer.GetSubscriber();
        await sub.SubscribeAsync(channel, handle);
    }

    /// <summary>
    /// 发布
    /// </summary>
    /// <param name="channel"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<long> PublishAsync(RedisChannel channel, RedisValue message)
    {
        var sub = _connMultiplexer.GetSubscriber();
        return await sub.PublishAsync(channel, message);
    }

    /// <summary>
    /// 发布（使用序列化）
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="channel"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<long> PublishAsync<T>(RedisChannel channel, T message)
    {
        var sub = _connMultiplexer.GetSubscriber();
        return await sub.PublishAsync(channel, Serialize(message));
    }

    #endregion 发布订阅-async

    #endregion 发布订阅


    /// <summary>
    /// 序列化s
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    private string Serialize(object obj)
    {
        if (obj == null)
            return null;

        return Newtonsoft.Json.JsonConvert.SerializeObject(obj, new Newtonsoft.Json.JsonSerializerSettings() { TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects, ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver() });
    }
}