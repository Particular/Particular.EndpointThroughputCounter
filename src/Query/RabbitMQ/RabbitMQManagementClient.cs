namespace Particular.ThroughputQuery.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
                var bindingsUrl = $"{ManagementUri}/api/queues/{HttpUtility.UrlEncode(queue.VHost)}/{queue.Name}/bindings";
                using (var stream = await http.GetStreamAsync(bindingsUrl, cancellationToken).ConfigureAwait(false))
                {
                    var bindings = JsonNode.Parse(stream)!.AsArray();
                    var conventionalBindingFound = bindings.Any(binding =>
                    {
                        return binding["source"]?.GetValue<string>() == queue.Name
                               && binding["vhost"]?.GetValue<string>() == queue.VHost
                               && binding["destination"]?.GetValue<string>() == queue.Name
                               && binding["destination_type"]?.GetValue<string>() == "queue"
                               && binding["routing_key"]?.GetValue<string>() == string.Empty
                               && binding["properties_key"]?.GetValue<string>() == "~";
                    });

                    if (conventionalBindingFound)
                    {
                        queue.EndpointIndicators.Add("ConventionalTopologyBinding");
                    }
                }

                var exchangeUrl = $"{ManagementUri}/api/exchanges/{HttpUtility.UrlEncode(queue.VHost)}/{queue.Name}/bindings/destination";
                using (var stream = await http.GetStreamAsync(exchangeUrl, cancellationToken).ConfigureAwait(false))
                {
                    var bindings = JsonNode.Parse(stream)!.AsArray();
                    var delayBindingFound = bindings.Any(binding =>
                    {
                        var source = binding["source"]?.GetValue<string>();

                        return (source == "nsb.v2.delay-delivery" || source == "nsb.delay-delivery")
                            && binding["vhost"]?.GetValue<string>() == queue.VHost
                            && binding["destination"]?.GetValue<string>() == queue.Name
                            && binding["destination_type"]?.GetValue<string>() == "exchange"
                            && binding["routing_key"]?.GetValue<string>() == $"#.{queue.Name}";
                    });

                    if (delayBindingFound)
                    {
                        queue.EndpointIndicators.Add("DelayBinding");
                    }
                }
            }
        }

        async Task<(RabbitMQQueueDetails[], bool morePages)> GetPage(int page, CancellationToken cancellationToken)
        {
            var url = $"{ManagementUri}/api/queues?page={page}&page_size=500&name=&use_regex=false&pagination=true";

            using var http = httpFactory();

            using (var stream = await http.GetStreamAsync(url, cancellationToken).ConfigureAwait(false))
            {
                var container = JsonNode.Parse(stream)!;

                if (container is JsonObject obj)
                {

                    var pageCount = obj["page_count"].GetValue<int>();
                    var pageReturned = obj["page"].GetValue<int>();

                    if (obj["items"] is not JsonArray items)
                    {
                        return (null, false);
                    }

                    var queues = items.Select(item => new RabbitMQQueueDetails(item)).ToArray();

                    return (queues, pageCount > pageReturned);
                }
                else if (container is JsonArray arr) // Older versions of RabbitMQ API did not have paging and returned the array of items directly
                {
                    var queues = arr.Select(item => new RabbitMQQueueDetails(item)).ToArray();

                    return (queues, false);
                }
                else
                {
                    throw new Exception($"Was not able to get list of queues from RabbitMQ broker. API call succeeded and deserialized but was of unexpected type '{container.GetType().FullName}'.");
                }
            }
        }

        public async Task<RabbitMQDetails> GetRabbitDetails(CancellationToken cancellationToken = default)
        {
            var url = $"{ManagementUri}/api/overview";

            using var http = httpFactory();

            try
            {
                using (var stream = await http.GetStreamAsync(url, cancellationToken).ConfigureAwait(false))
                {
                    var obj = JsonNode.Parse(stream)!.AsObject();

                    var statsDisabled = obj["disable_stats"]?.GetValue<bool>() ?? false;

                    if (statsDisabled)
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The RabbitMQ broker is configured with `management.disable_stats = true` or `management_agent.disable_metrics_collector = true` and as a result queue statistics cannot be collected using this tool. Consider changing the configuration of the RabbitMQ broker.");
                    }

                    var rabbitVersion = obj["rabbitmq_version"] ?? obj["product_version"];
                    var mgmtVersion = obj["management_version"];
                    var clusterName = obj["cluster_name"];

                    return new RabbitMQDetails
                    {
                        ClusterName = clusterName?.GetValue<string>() ?? "Unknown",
                        RabbitMQVersion = mgmtVersion?.GetValue<string>() ?? "Unknown",
                        ManagementVersion = mgmtVersion?.GetValue<string>() ?? "Unknown"
                    };
                }
            }
            catch (JsonException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {url} did not return a valid JSON response. Is the RabbitMQ server configured correctly?");
            }
            catch (HttpRequestException hx)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {url} did not respond. The exception message was: {hx.Message}");
            }
        }
    }
}