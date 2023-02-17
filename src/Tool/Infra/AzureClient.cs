namespace Particular.EndpointThroughputCounter.Infra
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus.Administration;
    using Azure.Monitor.Query;
    using Azure.Monitor.Query.Models;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class AzureClient
    {
        readonly string resourceId;
        readonly string subscriptionId;
        readonly ConnectionSet[] connections;
        readonly List<string> loginExceptions = new();

        Queue<ConnectionSet> connectionQueue;
        ConnectionSet current;

        public string FullyQualifiedNamespace { get; }

        public AzureClient(string resourceId, string serviceBusDomain)
        {
            this.resourceId = resourceId;

            var parts = resourceId.Split('/');
            if (parts.Length != 9 || parts[0] != string.Empty || parts[1] != "subscriptions" || parts[3] != "resourceGroups" || parts[5] != "providers" || parts[6] != "Microsoft.ServiceBus" || parts[7] != "namespaces")
            {
                throw new Exception("The provided --resourceId value does not look like an Azure Service Bus resourceId. A correct value should take the form '/subscriptions/{GUID}/resourceGroups/{NAME}/providers/Microsoft.ServiceBus/namespaces/{NAME}'.");
            }

            // May be useful in the future
            // var resourceGroup = parts[4];
            subscriptionId = parts[2];
            var name = parts[8];

            FullyQualifiedNamespace = $"{name}.{serviceBusDomain}";
            RunInfo.Add("AzureServiceBusNamespace", FullyQualifiedNamespace);

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
                    var result = await getData(cancellationToken);
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
                        throw new HaltException(HaltReason.Auth, msg);
                    }
                }
            }
        }

        bool NextConnection()
        {
            if (connectionQueue.TryDequeue(out current))
            {
                Out.WriteLine($" - Attempting login with {current.Name}");
                return true;
            }

            current = null;
            return false;
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
                    token);

                // Yeah, it's buried deep
                var metricValues = response.Value.Metrics.FirstOrDefault()?.TimeSeries.FirstOrDefault()?.Values;
                return metricValues;
            }, cancellationToken);
        }

        public Task<string[]> GetQueueNames(CancellationToken cancellationToken = default)
        {
            Out.WriteLine("Connecting to ServiceBusAdministration to discover queue names...");

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

        [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Shouldn't need this, but don't want to delete just yet")]
        void OutputCliCredentialsDiagnostics()
        {
            const string endMsg = " It's possible that authentication will not work. If not, try re-authenticating with the Azure CLI as described in https://docs.particular.net/nservicebus/throughput-tool/azure-service-bus#prerequisites";

            try
            {
                var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                var settingsPath = Path.Combine(home, ".azure");

                if (!Directory.Exists(settingsPath))
                {
                    Out.WriteWarn("Could not locate $HOME/.azure directory, it's possible that authentication will not work.");
                    return;
                }

                var configPath = Path.Combine(settingsPath, "config");
                var cloudConfigPath = Path.Combine(settingsPath, "clouds.config");
                var profilePath = Path.Combine(settingsPath, "azureProfile.json");

                if (!File.Exists(cloudConfigPath) || !File.Exists(profilePath) || !File.Exists(configPath))
                {
                    Out.WriteWarn("Could not locate configuration files in $HOME/.azure directory." + endMsg);
                    return;
                }

                var cloudName = new ConfigurationBuilder()
                    .SetBasePath(settingsPath)
                    .AddIniFile("config")
                    .Build()
                    .GetValue<string>("cloud:name");

                if (string.IsNullOrEmpty(cloudName))
                {
                    Out.WriteWarn("Could not determine Azure cloud name." + endMsg);
                    return;
                }

                var currentSubscriptionId = new ConfigurationBuilder()
                    .SetBasePath(settingsPath)
                    .AddIniFile("clouds.config")
                    .Build()
                    .GetValue<string>($"{cloudName}:subscription");

                if (string.IsNullOrEmpty(currentSubscriptionId))
                {
                    Out.WriteWarn("Could not determine current subscription." + endMsg);
                    return;
                }

                var profileDoc = JsonConvert.DeserializeObject<JObject>(File.ReadAllText(profilePath));
                if (profileDoc is null)
                {
                    Out.WriteWarn("Could not read Azure CLI profile information." + endMsg);
                    return;
                }

                if (profileDoc.SelectToken($"$.subscriptions[?(@.id == '{currentSubscriptionId}')]") is not JObject currentSubscription)
                {
                    Out.WriteWarn("Could not find current subscription in Azure CLI profile." + endMsg);
                    return;
                }

                var subName = currentSubscription.Value<string>("name");
                var user = currentSubscription["user"].Value<string>("name");

                if (subName is not null && user is not null)
                {
                    Out.WriteLine($"Using Azure CLI credentials to authenticate to {cloudName} subscription '{subName}' as user '{user}'");
                }
                else
                {
                    Out.WriteWarn("Could not determine Azure subscription name or login user." + endMsg);
                    return;
                }

                if (currentSubscriptionId != subscriptionId)
                {
                    Out.WriteWarn("The current subscriptionId does not appear to match the subsciptionId in provided Azure Service Bus Resource ID." + endMsg);
                }
            }
            catch (Exception x)
            {
                Out.WriteWarn($"An error occurred trying to validate Azure Service Bus login information.{endMsg}{Environment.NewLine}Original exception message: {x.Message}");
            }
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
