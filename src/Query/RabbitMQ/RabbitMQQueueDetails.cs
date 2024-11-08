namespace Particular.ThroughputQuery.RabbitMQ
{
    using System.Collections.Generic;
    using System.Text.Json;

    public class RabbitMQQueueDetails
    {
        public RabbitMQQueueDetails(JsonElement token)
        {
            Name = token.GetProperty("name").GetString();
            VHost = token.GetProperty("vhost").GetString();
            if (token.TryGetProperty("message_stats", out var stats) && stats.TryGetProperty("ack", out var val))
            {
                AckedMessages = val.GetInt64();
            }
        }

        public string Id => $"{VHost}:{Name}";

        public string Name { get; }
        public string VHost { get; }
        public long? AckedMessages { get; }
        public List<string> EndpointIndicators { get; } = [];
    }
}