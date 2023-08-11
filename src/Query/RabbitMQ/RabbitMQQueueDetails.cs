namespace Particular.ThroughputQuery.RabbitMQ
{
    using Newtonsoft.Json.Linq;

    public class RabbitMQQueueDetails
    {
        public RabbitMQQueueDetails(JToken token)
        {
            Name = token["name"].Value<string>();
            if (token["message_stats"] is JObject stats && stats["ack"] is JValue val)
            {
                AckedMessages = val.Value<long>();
            }
        }
        public string Name { get; }
        public long? AckedMessages { get; }
    }
}