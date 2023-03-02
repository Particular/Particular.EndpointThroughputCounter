namespace Particular.ThroughputQuery
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus.Administration;
    using Azure.Monitor.Query;
    using Azure.Monitor.Query.Models;

    public class AzureClient
    {
        readonly string resourceId;
        readonly ConnectionSet[] connections;
        readonly List<string> loginExceptions = new();
        readonly Action<string> log;

        Queue<ConnectionSet> connectionQueue;
        ConnectionSet current;

        public string FullyQualifiedNamespace { get; }
        public string SubscriptionId { get; }
        public string ResourceGroup { get; }

        public AzureClient(string resourceId, string serviceBusDomain, Action<string> log = null)
        {
            this.resourceId = resourceId;
            this.log = log ?? new(msg => { });

            var parts = resourceId.Split('/');
            if (parts.Length != 9 || parts[0] != string.Empty || parts[1] != "subscriptions" || parts[3] != "resourceGroups" || parts[5] != "providers" || parts[6] != "Microsoft.ServiceBus" || parts[7] != "namespaces")
            {
                throw new Exception("The provided --resourceId value does not look like an Azure Service Bus resourceId. A correct value should take the form '/subscriptions/{GUID}/resourceGroups/{NAME}/providers/Microsoft.ServiceBus/namespaces/{NAME}'.");
            }

            ResourceGroup = parts[4];
            SubscriptionId = parts[2];
            var name = parts[8];

            FullyQualifiedNamespace = $"{name}.{serviceBusDomain}";

            connections = CreateCredentials()
                .Select(c => new ConnectionSet(c, FullyQualifiedNamespace))
                .ToArray();

            ResetConnectionQueue();
        }

        IEnumerable<TokenCredential> CreateCredentials()
        {
            yield return new AzureCliCredential();
            yield return new AzurePowerShellCredential();
            yield return new EnvironmentCredential();
            yield return new SharedTokenCacheCredential();
            yield return new VisualStudioCredential();
            yield return new VisualStudioCodeCredential();

            // Don't really need this one to take 100s * 4 tries to finally time out
            var opts = new TokenCredentialOptions();
            opts.Retry.MaxRetries = 1;
            opts.Retry.NetworkTimeout = TimeSpan.FromSeconds(10);
            yield return new ManagedIdentityCredential(FullyQualifiedNamespace, opts);
        }

        /// <summary>
        /// Doesn't change the last successful `current` method but restores all options as possibilities if it doesn't work
        /// </summary>
        public void ResetConnectionQueue()
        {
            connectionQueue = new Queue<ConnectionSet>(connections);
        }

        async Task<T> GetDataWithCurrentCredentials<T>(GetDataDelegate<T> getData, CancellationToken cancellationToken)
        {
            if (current is null)
            {
                _ = NextConnection();
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
                    loginExceptions.Add($"{Environment.NewLine} * {current.Name}: {x.Message}");
                    if (!NextConnection())
                    {
                        var allExceptionMessages = string.Join(string.Empty, loginExceptions);
                        var msg = "Unable to log in to Azure service using multiple credential types. The exception messages for each credential type (including help links) are provided below:"
                            + Environment.NewLine + allExceptionMessages;
                        throw new QueryException(QueryFailureReason.Auth, msg);
                    }
                }
            }
        }

        bool NextConnection()
        {
            try
            {
                current = connectionQueue.Dequeue();
                log($" - Attempting login with {current.Name}");
                return true;
            }
            catch (InvalidOperationException)
            {
                current = null;
                return false;
            }
        }

        public Task<IReadOnlyList<MetricValue>> GetMetrics(string queueName, DateTime startTime, DateTime endTime, CancellationToken cancellationToken = default)
        {
            return GetDataWithCurrentCredentials(async token =>
            {
                var response = await current.Metrics.QueryResourceAsync(resourceId,
                    new[] { "CompleteMessage" },
                    new MetricsQueryOptions
                    {
                        Filter = $"EntityName eq '{queueName}'",
                        TimeRange = new QueryTimeRange(startTime, endTime),
                        Granularity = TimeSpan.FromDays(1)
                    },
                    token).ConfigureAwait(false);

                // Yeah, it's buried deep
                var metricValues = response.Value.Metrics.FirstOrDefault()?.TimeSeries.FirstOrDefault()?.Values;
                return metricValues;
            }, cancellationToken);
        }

        public Task<string[]> GetQueueNames(CancellationToken cancellationToken = default)
        {
            log("Connecting to ServiceBusAdministration to discover queue names...");

            return GetDataWithCurrentCredentials(async token =>
            {
                var queueList = new List<string>();
                await foreach (var queue in current.ServiceBus.GetQueuesAsync(cancellationToken).WithCancellation(cancellationToken))
                {
                    queueList.Add(queue.Name);
                }

                return queueList
                    .OrderBy(name => name)
                    .ToArray();
            }, cancellationToken);
        }

        static bool IsAuthenticationException(Exception x)
        {
            return x is CredentialUnavailableException or AuthenticationFailedException or UnauthorizedAccessException;
        }

        delegate Task<T> GetDataDelegate<T>(CancellationToken cancellationToken);

        class ConnectionSet
        {
            public string Name { get; }
            public MetricsQueryClient Metrics { get; }
            public ServiceBusAdministrationClient ServiceBus { get; }

            public ConnectionSet(TokenCredential credentials, string fullyQualifiedNamespace)
            {
                Name = credentials.GetType().Name;
                Metrics = new MetricsQueryClient(credentials);
                ServiceBus = new ServiceBusAdministrationClient(fullyQualifiedNamespace, credentials);
            }
        }
    }
}
