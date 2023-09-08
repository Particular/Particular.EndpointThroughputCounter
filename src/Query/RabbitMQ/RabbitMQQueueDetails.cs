namespace Particular.ThroughputQuery.RabbitMQ
{
    using System.Collections.Generic;
    using System.Text.Json.Nodes;

    public class RabbitMQQueueDetails
    {
        public RabbitMQQueueDetails(JsonNode token)
        {
            Name = token["name"].GetValue<string>();
            VHost = token["vhost"].GetValue<string>();
            if (token["message_stats"] is JsonObject stats && stats["ack"] is JsonValue val)
            {
                AckedMessages = val.GetValue<long>();
            }
        }
        public string Name { get; }
        public string VHost { get; }
        public long? AckedMessages { get; }
        public List<string> EndpointIndicators { get; } = new List<string>();
    }
}