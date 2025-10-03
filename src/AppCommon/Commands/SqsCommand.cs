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
            var datapoints = (await aws.GetMMetricsData(queueName, cancellationToken)).OrderBy(d => d.Timestamp).ToArray();

            var maxThroughput = datapoints is { Length: > 0 } ?
                                (long)datapoints.Select(d => d.Sum.GetValueOrDefault(0)).Max() : 0L;
            // Since we get 365 days of data, if there's no throughput in that amount of time, hard to legitimately call it an endpoint
            if (maxThroughput > 0)
            {
                var startTime = DateOnly.FromDateTime(datapoints.First().Timestamp.Value);
                var endTime = DateOnly.FromDateTime(datapoints.Last().Timestamp.Value);
                DateOnly currentDate = startTime;
                var dailyData = new Dictionary<DateOnly, DailyThroughput>();
                while (currentDate <= endTime)
                {
                    dailyData.Add(currentDate, new DailyThroughput { MessageCount = 0, DateUTC = currentDate });

                    currentDate = currentDate.AddDays(1);
                }

                foreach (var datapoint in datapoints)
                {
                    // There is a bug in the AWS SDK. The timestamp is actually UTC time, eventhough the DateTime returned type says Local
                    // See https://github.com/aws/aws-sdk-net/issues/167
                    // So do not convert the timestamp to UTC time!
                    if (datapoint.Timestamp.HasValue)
                    {
                        currentDate = DateOnly.FromDateTime(datapoint.Timestamp.Value);
                        dailyData[currentDate] = new DailyThroughput { MessageCount = (long)datapoint.Sum.GetValueOrDefault(0), DateUTC = currentDate };
                    }
                }

                data.Add(new QueueThroughput
                {
                    QueueName = queueName,
                    Throughput = maxThroughput,
                    DailyThroughputFromBroker = [.. dailyData.Values]
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