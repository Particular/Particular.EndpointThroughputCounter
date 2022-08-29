using Newtonsoft.Json.Linq;

class RabbitQueueDetails
{
    public RabbitQueueDetails(JToken token)
    {
        Name = token["name"].Value<string>();
        Messages = token["messages"].Value<int>();
        if (token["message_stats"] is JObject stats)
        {
            AckedMessages = stats["ack"].Value<int>();
        }
    }
    public string Name { get; }
    public int Messages { get; }
    public int AckedMessages { get; }
}