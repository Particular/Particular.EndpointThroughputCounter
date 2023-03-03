namespace Particular.ThroughputQuery
{
    using Newtonsoft.Json.Linq;

    public class RabbitQueueDetails
    {
        public RabbitQueueDetails(JToken token)
        {
            Name = token["name"].Value<string>();
            if (token["message_stats"] is JObject stats && stats["ack"] is JValue val)
            {
                AckedMessages = val.Value<int>();
            }
        }
        public string Name { get; }
        public int? AckedMessages { get; }
    }
}