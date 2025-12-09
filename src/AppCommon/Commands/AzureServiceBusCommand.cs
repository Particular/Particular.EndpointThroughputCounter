using System.CommandLine;
using Particular.EndpointThroughputCounter.Infra;
using Particular.LicensingComponent.Report;
using Particular.ThroughputQuery;
using Particular.ThroughputQuery.AzureServiceBus;

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

        var serviceBusDomainArg = new Option<string>("--serviceBusDomain",
            description: "The Service Bus domain. Defaults to 'servicebus.windows.net' and only must be specified for Azure customers using non-standard domains like government cloud customers.")
        {
            IsRequired = false
        };

        var regionArg = new Option<string>(
            name: "--region",
            description: "The Azure region where the Service Bus namespace is located, which is listed as the location in the Properties page in the Azure Portal.")
        {
            IsRequired = true
        };

        var metricsDomainArg = new Option<string>("--metricsDomain",
            description: "The Azure Monitor Metrics domain. Defaults to 'metrics.monitor.azure.com' and only must be specified for Azure customers using non-standard domains like government cloud customers.")
        {
            IsRequired = false
        };

        serviceBusDomainArg.SetDefaultValue("servicebus.windows.net");
        metricsDomainArg.SetDefaultValue("metrics.monitor.azure.com");

        command.AddOption(resourceIdArg);
        command.AddOption(serviceBusDomainArg);
        command.AddOption(regionArg);
        command.AddOption(metricsDomainArg);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var resourceId = context.ParseResult.GetValueForOption(resourceIdArg);
            var serviceBusDomain = context.ParseResult.GetValueForOption(serviceBusDomainArg);
            var region = context.ParseResult.GetValueForOption(regionArg);
            var metricsDomain = context.ParseResult.GetValueForOption(metricsDomainArg);
            var cancellationToken = context.GetCancellationToken();

#if DEBUG
            if (resourceId == "LOAD_FROM_CONFIG")
            {
                // So we don't have to keep an Azure Service Bus resource id in launchSettings.json
                // Create a local.settings.json file with the key below.
                resourceId = AppConfig.Get<string>("AZURESERVICEBUS_RESOURCE_ID");
            }
#endif

            var runner = new AzureServiceBusCommand(shared, resourceId, serviceBusDomain, region, metricsDomain);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly AzureClient azure;

    string[] queueNames;

    public AzureServiceBusCommand(SharedOptions shared, string resourceId, string serviceBusDomain, string region, string metricsDomain)
        : base(shared)
    {
        azure = new AzureClient(resourceId, serviceBusDomain, region, metricsDomain, Out.WriteLine);
        RunInfo.Add("AzureServiceBusNamespace", azure.FullyQualifiedNamespace);
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        try
        {
            var endTime = DateOnly.FromDateTime(DateTime.UtcNow);
            var startTime = endTime.AddDays(-90);
            var results = new List<QueueThroughput>();

            azure.ResetConnectionQueue();
            Out.WriteLine("Connecting to Azure Metrics to get throughput data...");

            for (var i = 0; i < queueNames.Length; i++)
            {
                var queueName = queueNames[i];

                Out.Write($"Gathering metrics for queue {i + 1}/{queueNames.Length}: {queueName}");

                var metricValues = (await azure.GetMetrics(queueName, startTime, endTime, cancellationToken)).OrderBy(m => m.TimeStamp).ToArray();

                var maxThroughput = metricValues.Select(timeEntry => timeEntry.Total).Max();

                // Since we get 90 days of data, if there's no throughput in that amount of time, hard to legitimately call it an endpoint
                if (maxThroughput is not null and not 0)
                {
                    var start = DateOnly.FromDateTime(metricValues.First().TimeStamp.UtcDateTime);
                    var end = DateOnly.FromDateTime(metricValues.Last().TimeStamp.UtcDateTime);
                    var currentDate = start;
                    var data = new Dictionary<DateOnly, DailyThroughput>();
                    while (currentDate <= end)
                    {
                        data.Add(currentDate, new DailyThroughput
                        {
                            MessageCount = 0,
                            DateUTC = currentDate
                        });

                        currentDate = currentDate.AddDays(1);
                    }

                    foreach (var metricValue in metricValues)
                    {
                        currentDate = DateOnly.FromDateTime(metricValue.TimeStamp.UtcDateTime);
                        data[currentDate] = new DailyThroughput
                        {
                            MessageCount = (long)(metricValue.Total ?? 0),
                            DateUTC = currentDate
                        };
                    }

                    results.Add(new QueueThroughput
                    {
                        QueueName = queueName,
                        Throughput = (long?)maxThroughput,
                        DailyThroughputFromBroker = [.. data.Values]
                    });
                    Out.WriteLine($" - Max daily throughput: {maxThroughput} ({start.ToShortDateString()} - {end.ToShortDateString()})");
                }
                else
                {
                    Out.WriteLine(" - No throughput detected in 90 days, ignoring");
                }
            }

            var s = new DateTimeOffset(startTime, TimeOnly.MinValue, TimeSpan.Zero);
            var e = new DateTimeOffset(endTime, TimeOnly.MaxValue, TimeSpan.Zero);
            return new QueueDetails
            {
                StartTime = s,
                EndTime = e,
                Queues = results.OrderBy(q => q.QueueName).ToArray(),
                TimeOfObservation = e - s
            };
        }
        catch (QueryException x)
        {
            throw new HaltException(x);
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        Out.WriteLine($"Getting data from {azure.FullyQualifiedNamespace}...");

        queueNames = await azure.GetQueueNames(cancellationToken);

        Out.WriteLine($"Found {queueNames.Length} queues");

        return new EnvironmentDetails
        {
            MessageTransport = "AzureServiceBus",
            ReportMethod = $"AzureServiceBus Metrics: {azure.FullyQualifiedNamespace}",
            QueueNames = queueNames,
            SkipEndpointListCheck = true
        };
    }
}