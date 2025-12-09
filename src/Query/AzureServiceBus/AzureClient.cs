namespace Particular.ThroughputQuery.AzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus.Administration;
    using Azure.Monitor.Query.Metrics;
    using Azure.Monitor.Query.Metrics.Models;

    public class AzureClient
    {
        readonly ResourceIdentifier resourceId;
        readonly AuthenticatedClientSet[] connections;
        readonly List<string> loginExceptions = [];
        readonly Action<string> log;

        Queue<AuthenticatedClientSet> connectionQueue;
        AuthenticatedClientSet currentClients;
        const string CompleteMessageMetricName = "CompleteMessage";
        const string MicrosoftServicebusNamespacesMetricsNamespace = "Microsoft.ServiceBus/Namespaces";

        public string FullyQualifiedNamespace { get; }

        public AzureClient(string resourceId, string serviceBusDomain, string region, string metricsDomain, Action<string> log = null)
        {
            this.resourceId = ResourceIdentifier.Parse(resourceId);

            this.log = log ?? (_ => { });

            FullyQualifiedNamespace = $"{this.resourceId.Name}.{serviceBusDomain}";

            connections = [.. CreateCredentials().Select(c => new AuthenticatedClientSet(c, FullyQualifiedNamespace, region, metricsDomain))];

            ResetConnectionQueue();
        }

        IEnumerable<TokenCredential> CreateCredentials()
        {
            yield return new AzureCliCredential();
            yield return new AzurePowerShellCredential();
            yield return new EnvironmentCredential();
            yield return new VisualStudioCredential();

            // Don't really need this one to take 100s * 4 tries to finally time out
            var opts = new TokenCredentialOptions
            {
                Retry =
                {
                    MaxRetries = 1,
                    NetworkTimeout = TimeSpan.FromSeconds(10)
                }
            };
            yield return new ManagedIdentityCredential(FullyQualifiedNamespace, opts);
        }

        /// <summary>
        /// Doesn't change the last successful `current` method but restores all options as possibilities if it doesn't work
        /// </summary>
        public void ResetConnectionQueue() => connectionQueue = new Queue<AuthenticatedClientSet>(connections);

        async Task<T> GetDataWithCurrentCredentials<T>(GetDataDelegate<T> getData, CancellationToken cancellationToken)
        {
            if (currentClients is null)
            {
                _ = NextCredentials();
            }

            while (true)
            {
                try
                {
                    var result = await getData(cancellationToken).ConfigureAwait(false);
                    return result;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception x) when (IsAuthenticationException(x))
                {
                    loginExceptions.Add($"{Environment.NewLine} * {currentClients.Name}: {x.Message}");
                    if (!NextCredentials())
                    {
                        var allExceptionMessages = string.Join(string.Empty, loginExceptions);
                        var msg = "Unable to log in to Azure service using multiple credential types. The exception messages for each credential type (including help links) are provided below:"
                                  + Environment.NewLine + allExceptionMessages;
                        throw new QueryException(QueryFailureReason.Auth, msg);
                    }
                }
            }
        }

        bool NextCredentials()
        {
            try
            {
                currentClients = connectionQueue.Dequeue();
                log($" - Attempting login with {currentClients.Name}");
                return true;
            }
            catch (InvalidOperationException)
            {
                currentClients = null;
                return false;
            }
        }

        public Task<IList<MetricValue>> GetMetrics(string queueName, DateOnly startTime, DateOnly endTime, CancellationToken cancellationToken = default) =>
            GetDataWithCurrentCredentials(async token =>
            {
                try
                {
                    var response = await currentClients.Metrics.QueryResourcesAsync(
                        [resourceId],
                        [CompleteMessageMetricName],
                        MicrosoftServicebusNamespacesMetricsNamespace,
                        new MetricsQueryResourcesOptions
                        {
                            Filter = $"EntityName eq '{queueName}'",
                            StartTime = startTime.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
                            EndTime = endTime.ToDateTime(TimeOnly.MaxValue, DateTimeKind.Utc),
                            Granularity = TimeSpan.FromDays(1)
                        },
                        token).ConfigureAwait(false);

                    var metricQueryResult = response.Value.Values.SingleOrDefault(mr => mr.Namespace == MicrosoftServicebusNamespacesMetricsNamespace);

                    if (metricQueryResult is null)
                    {
                        throw new Exception("No metrics query results returned for Microsoft.ServiceBus/Namespace");
                    }

                    var metricResult = metricQueryResult.GetMetricByName(CompleteMessageMetricName);

                    if (metricResult.Error.Message is not null)
                    {
                        throw new Exception($"Metrics query result for '{metricResult.Name}' failed: {metricResult.Error.Message}");
                    }

                    var timeSeries = metricResult.TimeSeries.SingleOrDefault();

                    if (timeSeries is null)
                    {
                        throw new Exception($"Metrics query result for '{metricResult.Name}' contained no time series");
                    }

                    return timeSeries.Values;
                }
                catch (Azure.RequestFailedException reqFailed) when (reqFailed.Message.Contains("ResourceGroupNotFound"))
                {
                    // Azure exception message has a lot of information including exact resource group name
                    throw new QueryException(QueryFailureReason.InvalidEnvironment, reqFailed.Message);
                }
            }, cancellationToken);

        public Task<string[]> GetQueueNames(CancellationToken cancellationToken = default)
        {
            log("Connecting to ServiceBusAdministration to discover queue names...");

            return GetDataWithCurrentCredentials(async token =>
            {
                var queueList = new List<string>();
                await foreach (var queue in currentClients.ServiceBus.GetQueuesAsync(token))
                {
                    queueList.Add(queue.Name);
                }

                return queueList
                    .OrderBy(name => name)
                    .ToArray();
            }, cancellationToken);
        }

        static bool IsAuthenticationException(Exception x) =>
            x is CredentialUnavailableException or AuthenticationFailedException or UnauthorizedAccessException;

        delegate Task<T> GetDataDelegate<T>(CancellationToken cancellationToken);

        class AuthenticatedClientSet
        {
            public string Name { get; }
            public MetricsClient Metrics { get; }
            public ServiceBusAdministrationClient ServiceBus { get; }

            public AuthenticatedClientSet(TokenCredential credentials, string fullyQualifiedNamespace, string region, string metricsDomain)
            {
                var metricsUrl = $"https://{region}.{metricsDomain}";
                var audience = $"https://{metricsDomain}";

                Name = credentials.GetType().Name;
                Metrics = new MetricsClient(new Uri(metricsUrl), credentials, new MetricsClientOptions { Audience = new MetricsClientAudience(audience) });
                ServiceBus = new ServiceBusAdministrationClient(fullyQualifiedNamespace, credentials);
            }
        }
    }
}