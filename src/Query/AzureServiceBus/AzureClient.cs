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
    using Azure.Monitor.Query;
    using Azure.Monitor.Query.Models;

    public class AzureClient
    {
        readonly string resourceId;
        readonly AuthenticatedClientSet[] connections;
        readonly List<string> loginExceptions = new();
        readonly Action<string> log;

        Queue<AuthenticatedClientSet> connectionQueue;
        AuthenticatedClientSet currentClients;

        public string FullyQualifiedNamespace { get; }
        public string SubscriptionId { get; }
        public string ResourceGroup { get; }

        public AzureClient(string resourceId, string serviceBusDomain, Action<string> log = null)
        {
            this.resourceId = resourceId;

            this.log = log ?? new(msg => { });

            AzureResourceId.Parse(resourceId,
                out var resourceIdSegments);

            ResourceGroup = resourceIdSegments.resourceGroup;

            SubscriptionId = resourceIdSegments.subscriptionId;

            FullyQualifiedNamespace = $"{resourceIdSegments.@namespace}.{serviceBusDomain}";

            connections = CreateCredentials()
                .Select(c => new AuthenticatedClientSet(c, FullyQualifiedNamespace))
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
            connectionQueue = new Queue<AuthenticatedClientSet>(connections);
        }

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

        public Task<IReadOnlyList<MetricValue>> GetMetrics(string queueName, DateTime startTime, DateTime endTime, CancellationToken cancellationToken = default)
        {
            return GetDataWithCurrentCredentials(async token =>
            {
                try
                {
                    var response = await currentClients.Metrics.QueryResourceAsync(resourceId,
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
                }
                catch (Azure.RequestFailedException reqFailed) when (reqFailed.Message.Contains("ResourceGroupNotFound"))
                {
                    // Azure exception message has a lot of information including exact resource group name
                    throw new QueryException(QueryFailureReason.InvalidEnvironment, reqFailed.Message);
                }
            }, cancellationToken);
        }

        public Task<string[]> GetQueueNames(CancellationToken cancellationToken = default)
        {
            log("Connecting to ServiceBusAdministration to discover queue names...");

            return GetDataWithCurrentCredentials(async token =>
            {
                var queueList = new List<string>();
                await foreach (var queue in currentClients.ServiceBus.GetQueuesAsync(cancellationToken).WithCancellation(cancellationToken))
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

        class AuthenticatedClientSet
        {
            public string Name { get; }
            public MetricsQueryClient Metrics { get; }
            public ServiceBusAdministrationClient ServiceBus { get; }

            public AuthenticatedClientSet(TokenCredential credentials, string fullyQualifiedNamespace)
            {
                var managementUrl = "https://management.azure.com";

                if (!fullyQualifiedNamespace.EndsWith("servicebus.windows.net", StringComparison.OrdinalIgnoreCase))
                {
                    var reversedParts = fullyQualifiedNamespace.Split('.', StringSplitOptions.RemoveEmptyEntries).Reverse().ToArray();
                    managementUrl = $"https://management.{reversedParts[1]}.{reversedParts[0]}";
                }

                Name = credentials.GetType().Name;
                Metrics = new MetricsQueryClient(new Uri(managementUrl), credentials);
                ServiceBus = new ServiceBusAdministrationClient(fullyQualifiedNamespace, credentials);
            }
        }
    }
}
