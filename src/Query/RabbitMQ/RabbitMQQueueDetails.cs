namespace Particular.ThroughputQuery.RabbitMQ
{
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;

    public class RabbitMQQueueDetails
    {
        public RabbitMQQueueDetails(JToken token)
        {
            Name = token["name"].Value<string>();
            VHost = token["vhost"].Value<string>();
            if (token["message_stats"] is JObject stats && stats["ack"] is JValue val)
            {
                AckedMessages = val.Value<long>();
            }
        }

        public string Id => $"{VHost}:{Name}";

        public string Name { get; }
        public string VHost { get; }
        public long? AckedMessages { get; }
        public List<string> EndpointIndicators { get; } = new List<string>();
    }
}