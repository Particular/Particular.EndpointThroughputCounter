using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Particular.EndpointThroughputCounter.Data;

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
    }

    string prefix;
    int metricsReceived;
    List<string> queueNames;
    string[] ignoredQueueNames;

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var cloudWatch = new AmazonCloudWatchClient();
        Out.WriteLine($"Loading CloudWatch metrics from {cloudWatch.Config.RegionEndpoint.SystemName}.");

        var data = new ConcurrentBag<QueueThroughput>();

        var tasks = queueNames.Select(async queueName =>
        {
            var req = new GetMetricStatisticsRequest
            {
                Namespace = "AWS/SQS",
                MetricName = "NumberOfMessagesDeleted",
                StartTimeUtc = startTime,
                EndTimeUtc = endTime,
                Period = 86400, // 1 day
                Statistics = new List<string> { "Sum" },
                Dimensions = new List<Dimension> {
                    new Dimension { Name = "QueueName", Value = queueName }
                }
            };

            var resp = await cloudWatch.GetMetricStatisticsAsync(req, cancellationToken);

            var maxThroughput = resp.Datapoints.OrderByDescending(d => d.Sum).FirstOrDefault()?.Sum ?? 0;

            data.Add(new QueueThroughput
            {
                QueueName = queueName,
                Throughput = (int)maxThroughput
            });

            Interlocked.Increment(ref metricsReceived);
            Out.Progress($"Got data for {metricsReceived}/{queueNames.Count} SQS queues.");
        });

        await Task.WhenAll(tasks);

        Out.EndProgress();

        return new QueueDetails
        {
            StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
            EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
            Queues = data.OrderBy(q => q.QueueName).ToArray(),
            TimeOfObservation = TimeSpan.FromDays(1)
        };
    }

    async Task GetQueues(CancellationToken cancellationToken)
    {
        var sqs = new AmazonSQSClient();

        Out.WriteLine($"Loading SQS queue names from {sqs.Config.RegionEndpoint.SystemName}.");

        var request = new ListQueuesRequest
        {
            MaxResults = 1000
        };

        queueNames = new List<string>();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await sqs.ListQueuesAsync(request, cancellationToken);

            queueNames.AddRange(response.QueueUrls.Select(url => url.Split('/')[4]));

            Out.Progress($"Found {queueNames.Count} SQS queues.");

            if (response.NextToken is not null)
            {
                request.NextToken = response.NextToken;
            }
            else
            {
                Out.WriteLine();
                break;
            }
        }
        Out.EndProgress();

        ignoredQueueNames = queueNames
            .Where(name => prefix is not null && !name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(name => name)
            .ToArray();

        if (ignoredQueueNames.Any())
        {
            var hash = ignoredQueueNames.ToHashSet();
            queueNames.RemoveAll(name => hash.Contains(name));

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