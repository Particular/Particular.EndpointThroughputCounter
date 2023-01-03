using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

    public async Task<string> GetRabbitDetails(CancellationToken cancellationToken = default)
    {
        var url = $"{managementUri}/api/overview";

        using (var stream = await http.GetStreamAsync(url, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var obj = serializer.Deserialize<JObject>(jsonReader);

            var statsDisabled = obj["disable_stats"].Value<bool>();

            if (statsDisabled)
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"The RabbitMQ broker is configured with `management.disable_stats = true` or `management_agent.disable_metrics_collector = true` and as a result queue statistics cannot be collected using this tool. Consider changing the configuration of the RabbitMQ broker.");
            }

            var rabbitVersion = (obj["rabbitmq_version"] ?? obj["product_version"])?.Value<string>() ?? "Unknown";
            var mgmtVersion = obj["management_version"]?.Value<string>() ?? "Unknown";
            var clusterName = obj["cluster_name"]?.Value<string>() ?? "Unknown";

            return $"RabbitMQ v{rabbitVersion}, Mgmt v{mgmtVersion}, Cluster {clusterName}";
        }
    }
}