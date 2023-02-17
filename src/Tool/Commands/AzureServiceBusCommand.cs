using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Particular.EndpointThroughputCounter.Data;
using Particular.EndpointThroughputCounter.Infra;

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

#if DEBUG
            if (resourceId == "LOAD_FROM_CONFIG")
            {
                // So we don't have to keep an Azure Service Bus resource id in launchSettings.json
                // Create a local.settings.json file with the key below.
                resourceId = AppConfig.Get<string>("AZURESERVICEBUS_RESOURCE_ID");
            }
#endif

            var runner = new AzureServiceBusCommand(shared, resourceId, serviceBusDomain, authType);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly AzureClient azure;


    public AzureServiceBusCommand(SharedOptions shared, string resourceId, string serviceBusDomain, string authType)
    : base(shared)
    {
        azure = new AzureClient(resourceId, serviceBusDomain, authType);
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Out.WriteLine($"Getting data from {azure.FullyQualifiedNamespace}...");
        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var queueNames = await azure.GetQueueNames(cancellationToken);

        Out.WriteLine($"Found {queueNames.Length} queues");

        var results = new List<QueueThroughput>();

        for (var i = 0; i < queueNames.Length; i++)
        {
            var queueName = queueNames[i];

            Out.WriteLine($"Gathering metrics for queue {i + 1}/{queueNames.Length}: {queueName}");

            var metricValues = await azure.GetMetrics(queueName, startTime, endTime, cancellationToken);

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

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        azure.OutputCredentialDiagnostics();

        return Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "AzureServiceBus",
            ReportMethod = $"AzureServiceBus Metrics: {azure.FullyQualifiedNamespace}",
            SkipEndpointListCheck = true
        });
    }
}