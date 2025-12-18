using System.Collections.Concurrent;
using System.CommandLine;
using Amazon;
using Particular.LicensingComponent.Report;
using Particular.ThroughputQuery.AmazonSQS;

class SqsCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("sqs", "Measure endpoints and throughput using CloudWatch metrics for Amazon SQS");

        var profileOption = new Option<string>(
            name: "--profile",
            description: "The name of the credentials profile to use when accessing AWS services. If not provided, the default profile or environment variables will be used.");

        var regionOption = new Option<string>(
            name: "--region",
            description: "The AWS region to use when accessing AWS services. If not provided, the default profile or AWS_REGION environment variable will be used.");

        var prefixOption = new Option<string>("--prefix", "Only collect information for SQS queues matching a prefix, such as 'prod'");

        command.AddOption(profileOption);
        command.AddOption(regionOption);
        command.AddOption(prefixOption);

        command.SetHandler(async context =>
        {
            var profile = context.ParseResult.GetValueForOption(profileOption);
            var region = context.ParseResult.GetValueForOption(regionOption);
            var prefix = context.ParseResult.GetValueForOption(prefixOption);
            var shared = SharedOptions.Parse(context);
            var cancellationToken = context.GetCancellationToken();

            var runner = new SqsCommand(shared, profile, region, prefix);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    public SqsCommand(SharedOptions shared, string profile, string regionName, string prefix)
    : base(shared)
    {
        if (profile is not null)
        {
            Out.WriteLine($"Specifying credentials profile '{profile}'");
            AWSConfigs.AWSProfileName = profile;
        }

        if (regionName is not null)
        {
            var region = RegionEndpoint.GetBySystemName(regionName);
            Out.WriteLine($"Specifying region {region.SystemName} ({region.DisplayName})");
            AWSConfigs.RegionEndpoint = region;
        }

        this.prefix = prefix;
        aws = new AwsQuery();
    }

    readonly AwsQuery aws;
    readonly string prefix;
    int metricsReceived;
    List<string> queueNames;
    string[] ignoredQueueNames;

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Out.WriteLine($"Loading CloudWatch metrics from {aws.CloudWatchRegion}.");

        var data = new ConcurrentBag<QueueThroughput>();
        var numberOfQueues = queueNames.Count;

        var tasks = queueNames.Select(async queueName =>
        {
            var response = await aws.GetMMetricsData(queueName, cancellationToken);
            var datapoints = response.ToDictionary(
                k => DateOnly.FromDateTime(k.Timestamp.Value),
                v => (long)v.Sum.GetValueOrDefault(0)
            );

            var maxThroughput = datapoints.Values.Max();

            // Since we get 365 days of data, if there's no throughput in that amount of time, hard to legitimately call it an endpoint
            if (maxThroughput > 0)
            {
                var startTime = datapoints.Keys.Min();
                var endTime = datapoints.Keys.Max();

                var dailyData = new List<DailyThroughput>(endTime.DayNumber - startTime.DayNumber + 1);

                for (var currentDate = startTime; currentDate <= endTime; currentDate = currentDate.AddDays(1))
                {
                    datapoints.TryGetValue(currentDate, out var count);
                    dailyData.Add(new DailyThroughput
                    {
                        MessageCount = count,
                        DateUTC = currentDate
                    });
                }

                data.Add(new QueueThroughput
                {
                    QueueName = queueName,
                    Throughput = maxThroughput,
                    DailyThroughputFromBroker = [.. dailyData]
                });
            }

            var numberOfReceivedMetrics = Interlocked.Increment(ref metricsReceived);
            Out.Progress($"Got data for {numberOfReceivedMetrics}/{numberOfQueues} SQS queues.");
        });

        await Task.WhenAll(tasks);

        Out.EndProgress();

        var s = new DateTimeOffset(aws.StartDate.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
        var e = new DateTimeOffset(aws.EndDate.ToDateTime(TimeOnly.MaxValue), TimeSpan.Zero);
        return new QueueDetails
        {
            StartTime = s,
            EndTime = e,
            Queues = [.. data.OrderBy(q => q.QueueName)],
            TimeOfObservation = e - s
        };
    }

    async Task GetQueues(CancellationToken cancellationToken)
    {
        Out.WriteLine($"Loading SQS queue names from {aws.SQSRegion}.");

        queueNames = await aws.GetQueueNames(found => Out.Progress($"Found {found} SQS queues."), cancellationToken);
        Out.EndProgress();

        ignoredQueueNames = queueNames
            .Where(name => prefix is not null && !name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(name => name)
            .ToArray();

        if (ignoredQueueNames.Any())
        {
            var hash = ignoredQueueNames.ToHashSet();
            _ = queueNames.RemoveAll(name => hash.Contains(name));

            Out.WriteLine($"{queueNames.Count} queues match prefix '{prefix}'.");
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        await GetQueues(cancellationToken);

        return new EnvironmentDetails
        {
            MessageTransport = "AmazonSQS",
            ReportMethod = "AWS CloudWatch Metrics",
            QueueNames = queueNames.OrderBy(q => q).ToArray(),
            Prefix = prefix,
            IgnoredQueues = ignoredQueueNames.Any() ? ignoredQueueNames : null,
            SkipEndpointListCheck = true
        };
    }
}