using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Particular.EndpointThroughputCounter.Data;

class AzureServiceBusCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("azureservicebus", "Measure endpoints and throughput using Azure Service Bus metrics");

        var resourceIdArg = new Option<string>(
            name: "--resourceId",
            description: "The resource id for the Azure Service Bus namespace, which can be found in the Properties page in the Azure Portal.")
        {
            IsRequired = true
        };

        var authTypeArg = new Option<string>("--authType", "Specify the Azure authentication type.")
        {
            IsRequired = false
        };

        var serviceBusDomainArg = new Option<string>("--serviceBusDomain",
            description: "The Service Bus domain. Defaults to 'servicebus.windows.net' and only must be specified for Azure customers using non-standard domains like government cloud customers.")
        {
            IsRequired = false
        };

        serviceBusDomainArg.SetDefaultValue("servicebus.windows.net");

        command.AddOption(resourceIdArg);
        command.AddOption(authTypeArg);
        command.AddOption(serviceBusDomainArg);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var resourceId = context.ParseResult.GetValueForOption(resourceIdArg);
            var serviceBusDomain = context.ParseResult.GetValueForOption(serviceBusDomainArg);
            var authType = context.ParseResult.GetValueForOption(authTypeArg);
            var cancellationToken = context.GetCancellationToken();

            var runner = new AzureServiceBusCommand(shared, resourceId, serviceBusDomain, authType);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly string resourceId;
    readonly string fullyQualifiedNamespace;
    readonly TokenCredential credentials;
    readonly MetricsQueryClient metrics;
    readonly ServiceBusAdministrationClient serviceBusClient;

    public AzureServiceBusCommand(SharedOptions shared, string resourceId, string serviceBusDomain, string authType)
    : base(shared)
    {
        this.resourceId = resourceId;

        var parts = resourceId.Split('/');
        if (parts.Length != 9 || parts[0] != string.Empty || parts[1] != "subscriptions" || parts[3] != "resourceGroups" || parts[5] != "providers" || parts[6] != "Microsoft.ServiceBus" || parts[7] != "namespaces")
        {
            throw new Exception("The provided --resourceId value does not look like an Azure Service Bus resourceId. A correct value should take the form '/subscriptions/{GUID}/resourceGroups/{NAME}/providers/Microsoft.ServiceBus/namespaces/{NAME}'.");
        }

        // May be useful in the future
        // var resourceGroup = parts[4];
        var name = parts[8];

        fullyQualifiedNamespace = $"{name}.{serviceBusDomain}";

        credentials = authType switch
        {
            nameof(EnvironmentCredential) => new EnvironmentCredential(),
            nameof(ManagedIdentityCredential) => new ManagedIdentityCredential(),
            nameof(SharedTokenCacheCredential) => new SharedTokenCacheCredential(),
            nameof(VisualStudioCredential) => new VisualStudioCredential(),
            nameof(VisualStudioCodeCredential) => new VisualStudioCodeCredential(),
            nameof(AzureCliCredential) => new AzureCliCredential(),
            nameof(AzurePowerShellCredential) => new AzurePowerShellCredential(),
            //nameof(InteractiveBrowserCredential) => new InteractiveBrowserCredential(),
            _ => new AzureCliCredential()
        };
        metrics = new MetricsQueryClient(credentials);
        serviceBusClient = new ServiceBusAdministrationClient(fullyQualifiedNamespace, credentials);
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Out.WriteLine($"Getting data from {fullyQualifiedNamespace}...");
        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        try
        {
            var queueNames = await GetQueueNames(cancellationToken);

            Out.WriteLine($"Found {queueNames.Length} queues");

            var results = new List<QueueThroughput>();

            for (var i = 0; i < queueNames.Length; i++)
            {
                var queueName = queueNames[i];

                Out.WriteLine($"Gathering metrics for queue {i + 1}/{queueNames.Length}: {queueName}");

                var response = await metrics.QueryResourceAsync(resourceId,
                    new[] { "CompleteMessage" },
                    new MetricsQueryOptions
                    {
                        Filter = $"EntityName eq '{queueName}'",
                        TimeRange = new QueryTimeRange(startTime, endTime),
                        Granularity = TimeSpan.FromDays(1)
                    },
                    cancellationToken);

                // Yeah, it's buried deep
                var metricValues = response.Value.Metrics.FirstOrDefault()?.TimeSeries.FirstOrDefault()?.Values;

                if (metricValues is not null)
                {
                    var maxThroughput = metricValues.Select(timeEntry => timeEntry.Total).Max();
                    results.Add(new QueueThroughput { QueueName = queueName, Throughput = (int?)maxThroughput });
                }
            }

            return new QueueDetails
            {
                StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
                EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
                Queues = results.OrderBy(q => q.QueueName).ToArray(),
                TimeOfObservation = TimeSpan.FromDays(1)
            };
        }
        catch (CredentialUnavailableException x)
        {
            // Azure gives a good error message as part of the exception
            throw new HaltException(HaltReason.InvalidEnvironment, x.Message);
        }
    }

    async Task<string[]> GetQueueNames(CancellationToken cancellationToken)
    {
        Out.WriteLine($"Authenticating using {credentials.GetType().Name}");

        var queueList = new List<string>();

        await foreach (var queue in serviceBusClient.GetQueuesAsync(cancellationToken).WithCancellation(cancellationToken))
        {
            queueList.Add(queue.Name);
        }

        return queueList
            .OrderBy(name => name)
            .ToArray();
    }

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "AzureServiceBus",
            ReportMethod = $"AzureServiceBus Metrics: {fullyQualifiedNamespace}",
            SkipEndpointListCheck = true
        });
    }
}