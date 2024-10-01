namespace Particular.ThroughputQuery.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;

    public class RabbitMQManagementClient

    {
        readonly Func<HttpClient> httpFactory;

        public RabbitMQManagementClient(Func<HttpClient> httpFactory, string managementUri)
        {
            this.httpFactory = httpFactory;
            ManagementUri = managementUri.TrimEnd('/');
        }

        public string ManagementUri { get; }

        public async Task<List<RabbitMQQueueDetails>> GetQueueDetails(CancellationToken cancellationToken = default)
        {
            int page = 1;

            var results = new List<RabbitMQQueueDetails>();

            while (true)
            {
                var (queues, morePages) = await GetPage(page, cancellationToken).ConfigureAwait(false);

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

        public async Task AddAdditionalQueueDetails(List<RabbitMQQueueDetails> queues, CancellationToken cancellationToken = default)
        {
            using var http = httpFactory();
            foreach (var queue in queues)
            {
                try
                {
                    var bindingsUrl = $"{ManagementUri}/api/queues/{HttpUtility.UrlEncode(queue.VHost)}/{HttpUtility.UrlEncode(queue.Name)}/bindings";
                    using (var stream = await http.GetStreamAsync(bindingsUrl, cancellationToken).ConfigureAwait(false))
                    {
                        var bindings = JsonSerializer.Deserialize<JsonArray>(stream);
                        var conventionalBindingFound = bindings?.Any(binding => binding!["source"]?.GetValue<string>() == queue.Name
                                                                    && binding["vhost"]?.GetValue<string>() == queue.VHost
                                                                    && binding["destination"]?.GetValue<string>() == queue.Name
                                                                    && binding["destination_type"]?.GetValue<string>() == "queue"
                                                                    && binding["routing_key"]?.GetValue<string>() == string.Empty
                                                                    && binding["properties_key"]?.GetValue<string>() == "~") ?? false;

                        if (conventionalBindingFound)
                        {
                            queue.EndpointIndicators.Add("ConventionalTopologyBinding");
                        }
                    }
                }
                catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Clearly no conventional topology binding here
                }

                try
                {
                    var exchangeUrl = $"{ManagementUri}/api/exchanges/{HttpUtility.UrlEncode(queue.VHost)}/{HttpUtility.UrlEncode(queue.Name)}/bindings/destination";
                    using (var stream = await http.GetStreamAsync(exchangeUrl, cancellationToken).ConfigureAwait(false))
                    {
                        var bindings = JsonSerializer.Deserialize<JsonArray>(stream);

                        var delayBindingFound = bindings?.Any(binding =>
                        {
                            var source = binding!["source"]?.GetValue<string>();

                            return source is "nsb.v2.delay-delivery" or "nsb.delay-delivery"
                                && binding["vhost"]?.GetValue<string>() == queue.VHost
                                && binding["destination"]?.GetValue<string>() == queue.Name
                                && binding["destination_type"]?.GetValue<string>() == "exchange"
                                && binding["routing_key"]?.GetValue<string>() == $"#.{queue.Name}";
                        }) ?? false;

                        if (delayBindingFound)
                        {
                            queue.EndpointIndicators.Add("DelayBinding");
                        }
                    }
                }
                catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Clearly no delay binding here
                }
            }
        }

        async Task<(RabbitMQQueueDetails[], bool morePages)> GetPage(int page, CancellationToken cancellationToken)
        {
            var url = $"{ManagementUri}/api/queues?page={page}&page_size=500&name=&use_regex=false&pagination=true";

            using var http = httpFactory();

            using (var stream = await http.GetStreamAsync(url, cancellationToken).ConfigureAwait(false))
            {
                var container = JsonSerializer.Deserialize<JsonNode>(stream);

                switch (container)
                {
                    case JsonObject obj:
                        {
                            var pageCount = obj["page_count"]!.GetValue<int>();
                            var pageReturned = obj["page"]!.GetValue<int>();

                            if (obj["items"] is not JsonArray items)
                            {
                                return (null, false);
                            }

                            return (MaterializeQueueDetails(items), pageCount > pageReturned);
                        }
                    // Older versions of RabbitMQ API did not have paging and returned the array of items directly
                    case JsonArray arr:
                        {
                            return (MaterializeQueueDetails(arr), false);
                        }
                    default:
                        throw new Exception($"Was not able to get list of queues from RabbitMQ broker. API call succeeded and deserialized but was of unexpected type '{container.GetType().FullName}'.");
                }
            }
        }

        static RabbitMQQueueDetails[] MaterializeQueueDetails(JsonArray items)
        {
            // It is not possible to directly operated on the JsonNode. When the JsonNode is a JObject
            // and the indexer is access the internal dictionary is initialized which can cause key not found exceptions
            // when the payload contains the same key multiple times (which happened in the past).
            var queues = items.Select(item => new RabbitMQQueueDetails(item!.Deserialize<JsonElement>())).ToArray();
            return queues;
        }

        public async Task<RabbitMQDetails> GetRabbitDetails(CancellationToken cancellationToken = default)
        {
            var overviewUrl = $"{ManagementUri}/api/overview";

            var details = new RabbitMQDetails();

            using var http = httpFactory();

            try
            {
                using (var stream = await http.GetStreamAsync(overviewUrl, cancellationToken).ConfigureAwait(false))
                {
                    var obj = JsonSerializer.Deserialize<JsonObject>(stream);

                    var statsDisabled = obj["disable_stats"]?.GetValue<bool>() ?? false;

                    if (statsDisabled)
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The RabbitMQ broker is configured with `management.disable_stats = true` or `management_agent.disable_metrics_collector = true` and as a result queue statistics cannot be collected using this tool. Consider changing the configuration of the RabbitMQ broker.");
                    }

                    var rabbitVersion = obj["rabbitmq_version"] ?? obj["product_version"];
                    var mgmtVersion = obj["management_version"];
                    var clusterName = obj["cluster_name"];

                    details.ClusterName = clusterName?.GetValue<string>() ?? "Unknown";
                    details.RabbitMQVersion = rabbitVersion?.GetValue<string>() ?? "Unknown";
                    details.ManagementVersion = mgmtVersion?.GetValue<string>() ?? "Unknown";
                }
            }
            catch (JsonException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {overviewUrl} did not return a valid JSON response. Is the RabbitMQ server configured correctly?");
            }
            catch (HttpRequestException hx)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {overviewUrl} did not respond. The exception message was: {hx.Message}");
            }

            return details;
        }
    }
}