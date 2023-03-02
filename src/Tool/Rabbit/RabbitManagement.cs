using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

class RabbitManagement
{
    readonly HttpClient http;
    readonly JsonSerializer serializer;

    public RabbitManagement(HttpClient http, string managementUri)
    {
        this.http = http;
        ManagementUri = managementUri.TrimEnd('/');

        serializer = new JsonSerializer();
    }

    public string ManagementUri { get; }

    public async Task<List<RabbitQueueDetails>> GetQueueDetails(CancellationToken cancellationToken = default)
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
        var url = $"{ManagementUri}/api/queues?page={page}&page_size=500&name=&use_regex=false&pagination=true";

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

    public async Task<RabbitDetails> GetRabbitDetails(CancellationToken cancellationToken = default)
    {
        var url = $"{ManagementUri}/api/overview";

        try
        {
            using (var stream = await http.GetStreamAsync(url, cancellationToken))
            using (var reader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(reader))
            {
                var obj = serializer.Deserialize<JObject>(jsonReader);

                var statsDisabled = obj["disable_stats"]?.Value<bool>() ?? false;

                if (statsDisabled)
                {
                    throw new HaltException(HaltReason.InvalidEnvironment, $"The RabbitMQ broker is configured with `management.disable_stats = true` or `management_agent.disable_metrics_collector = true` and as a result queue statistics cannot be collected using this tool. Consider changing the configuration of the RabbitMQ broker.");
                }

                var rabbitVersion = obj["rabbitmq_version"] ?? obj["product_version"];
                var mgmtVersion = obj["management_version"];
                var clusterName = obj["cluster_name"];

                return new RabbitDetails
                {
                    ClusterName = clusterName?.Value<string>() ?? "Unknown",
                    RabbitVersion = mgmtVersion?.Value<string>() ?? "Unknown",
                    ManagementVersion = mgmtVersion?.Value<string>() ?? "Unknown"
                };
            }
        }
        catch (HttpRequestException hx)
        {
            throw new HaltException(HaltReason.InvalidEnvironment, $"The server at {url} did not respond. The exception message was: {hx.Message}");
        }
    }
}