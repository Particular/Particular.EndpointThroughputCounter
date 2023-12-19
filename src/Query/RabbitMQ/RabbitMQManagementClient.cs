namespace Particular.ThroughputQuery.RabbitMQ
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class RabbitMQManagementClient

    {
        readonly Func<HttpClient> httpFactory;
        readonly JsonSerializer serializer;

        public RabbitMQManagementClient(Func<HttpClient> httpFactory, string managementUri, string vhost)
        {
            this.httpFactory = httpFactory;
            ManagementUri = managementUri.TrimEnd('/');
            VHost = vhost;

            serializer = new JsonSerializer();
        }

        public string ManagementUri { get; }
        public string VHost { get; }

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
                    var bindingsUrl = $"{ManagementUri}/api/queues/{HttpUtility.UrlEncode(queue.VHost)}/{queue.Name}/bindings";
                    using (var stream = await http.GetStreamAsync(bindingsUrl, cancellationToken).ConfigureAwait(false))
                    using (var reader = new StreamReader(stream))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        var bindings = serializer.Deserialize<JArray>(jsonReader);
                        var conventionalBindingFound = bindings.Any(binding =>
                        {
                            return binding["source"]?.Value<string>() == queue.Name
                                && binding["vhost"]?.Value<string>() == queue.VHost
                                && binding["destination"]?.Value<string>() == queue.Name
                                && binding["destination_type"]?.Value<string>() == "queue"
                                && binding["routing_key"]?.Value<string>() == string.Empty
                                && binding["properties_key"]?.Value<string>() == "~";
                        });

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
                    var exchangeUrl = $"{ManagementUri}/api/exchanges/{HttpUtility.UrlEncode(queue.VHost)}/{queue.Name}/bindings/destination";
                    using (var stream = await http.GetStreamAsync(exchangeUrl, cancellationToken).ConfigureAwait(false))
                    using (var reader = new StreamReader(stream))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        var bindings = serializer.Deserialize<JArray>(jsonReader);
                        var delayBindingFound = bindings.Any(binding =>
                        {
                            var source = binding["source"]?.Value<string>();

                            return (source == "nsb.v2.delay-delivery" || source == "nsb.delay-delivery")
                                && binding["vhost"]?.Value<string>() == queue.VHost
                                && binding["destination"]?.Value<string>() == queue.Name
                                && binding["destination_type"]?.Value<string>() == "exchange"
                                && binding["routing_key"]?.Value<string>() == $"#.{queue.Name}";
                        });

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
            var vhostSegment = VHost is null ? string.Empty : $"/{WebUtility.UrlEncode(VHost)}";
            var url = $"{ManagementUri}/api/queues{vhostSegment}?page={page}&page_size=500&name=&use_regex=false&pagination=true";

            using var http = httpFactory();

            using (var stream = await http.GetStreamAsync(url, cancellationToken).ConfigureAwait(false))
            using (var reader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(reader))
            {
                var container = serializer.Deserialize<JContainer>(jsonReader);

                if (container is JObject obj)
                {

                    var pageCount = obj["page_count"].Value<int>();
                    var pageReturned = obj["page"].Value<int>();

                    if (obj["items"] is not JArray items)
                    {
                        return (null, false);
                    }

                    var queues = items.Select(item => new RabbitMQQueueDetails(item)).ToArray();

                    return (queues, pageCount > pageReturned);
                }
                else if (container is JArray arr) // Older versions of RabbitMQ API did not have paging and returned the array of items directly
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

        public async Task<RabbitMQDetails> GetRabbitDetails(string vhost, CancellationToken cancellationToken = default)
        {
            var overviewUrl = $"{ManagementUri}/api/overview";

            var details = new RabbitMQDetails();

            using var http = httpFactory();

            try
            {
                using (var stream = await http.GetStreamAsync(overviewUrl, cancellationToken).ConfigureAwait(false))
                using (var reader = new StreamReader(stream))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var obj = serializer.Deserialize<JObject>(jsonReader);

                    var statsDisabled = obj["disable_stats"]?.Value<bool>() ?? false;

                    if (statsDisabled)
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The RabbitMQ broker is configured with `management.disable_stats = true` or `management_agent.disable_metrics_collector = true` and as a result queue statistics cannot be collected using this tool. Consider changing the configuration of the RabbitMQ broker.");
                    }

                    var rabbitVersion = obj["rabbitmq_version"] ?? obj["product_version"];
                    var mgmtVersion = obj["management_version"];
                    var clusterName = obj["cluster_name"];

                    details.ClusterName = clusterName?.Value<string>() ?? "Unknown";
                    details.RabbitMQVersion = mgmtVersion?.Value<string>() ?? "Unknown";
                    details.ManagementVersion = mgmtVersion?.Value<string>() ?? "Unknown";
                }
            }
            catch (JsonReaderException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {overviewUrl} did not return a valid JSON response. Is the RabbitMQ server configured correctly?");
            }
            catch (HttpRequestException hx)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {overviewUrl} did not respond. The exception message was: {hx.Message}");
            }

            var vhostUrl = $"{ManagementUri}/api/vhosts";

            try
            {
                using (var stream = await http.GetStreamAsync(vhostUrl, cancellationToken).ConfigureAwait(false))
                using (var reader = new StreamReader(stream))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var list = serializer.Deserialize<JArray>(jsonReader);

                    if (list.Count == 0)
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {vhostUrl} has no vhosts. Is the RabbitMQ server configured correctly?");
                    }

                    if (list.Count > 1 && string.IsNullOrEmpty(vhost))
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {vhostUrl} has multiple vhosts, but the --vhost parameter was not specified. Include the --vhost parameter with the name of the virtual host to measure.");
                    }

                    var vhostNode = string.IsNullOrEmpty(vhost)
                        ? list.SingleOrDefault()
                        : list.FirstOrDefault(vh => vh["name"].Value<string>() == vhost);

                    if (vhostNode == null)
                    {
                        throw new QueryException(QueryFailureReason.InvalidEnvironment, $"Could not find the vhost named '{vhost}' on the server at {vhostUrl}. Check the value of the --vhost parameter.");
                    }

                    details.VHost = vhostNode["name"].Value<string>();
                }
            }
            catch (JsonReaderException)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {vhostUrl} did not return a valid JSON response. Is the RabbitMQ server configured correctly?");
            }
            catch (HttpRequestException hx)
            {
                throw new QueryException(QueryFailureReason.InvalidEnvironment, $"The server at {vhostUrl} did not respond. The exception message was: {hx.Message}");
            }

            return details;
        }
    }
}