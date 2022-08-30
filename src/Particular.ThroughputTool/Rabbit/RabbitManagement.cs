using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

class RabbitManagement
{
    readonly string managementUri;
    readonly AuthenticatingHttpClient http;
    readonly JsonSerializer serializer;

    public RabbitManagement(string managementUri)
    {
        this.managementUri = managementUri;

        http = new AuthenticatingHttpClient();

        serializer = new JsonSerializer();
    }

    public async Task<List<RabbitQueueDetails>> GetThroughput(CancellationToken cancellationToken = default)
    {
        int page = 1;

        var results = new List<RabbitQueueDetails>();

        while (true)
        {
            var (queues, morePages) = await GetPage(page, cancellationToken);

            if (queues != null)
            {
                results.AddRange(queues);
            }

            if (morePages)
            {
                page++;
            }
            else
            {
                break;
            }
        }

        return results;
    }

    async Task<(RabbitQueueDetails[], bool morePages)> GetPage(int page, CancellationToken cancellationToken)
    {
        var url = $"{managementUri}/api/queues?page={page}&page_size=500&name=&use_regex=false&pagination=true";

        using (var stream = await http.GetStreamAsync(url, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var obj = serializer.Deserialize<JObject>(jsonReader);

            var pageCount = obj["page_count"].Value<int>();
            var pageReturned = obj["page"].Value<int>();

            if (obj["items"] is not JArray items)
            {
                return (null, false);
            }

            var queues = items.Select(item => new RabbitQueueDetails(item)).ToArray();

            return (queues, pageCount > pageReturned);
        }
    }

}