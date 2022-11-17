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

        command.AddOption(profileOption);

        var regionOption = new Option<string>(
            name: "--region",
            description: "The AWS region to use when accessing AWS services. If not provided, the default profile or AWS_REGION environment variable will be used.");

        command.SetHandler(async context =>
        {
            var profile = context.ParseResult.GetValueForOption(profileOption);
            var region = context.ParseResult.GetValueForOption(regionOption);
            var maskNames = context.ParseResult.GetValueForOption(SharedOptions.MaskNames);
            var cancellationToken = context.GetCancellationToken();

            var runner = new SqsCommand(profile, region, maskNames);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    public SqsCommand(string profile, string regionName, string[] maskNames)
    : base(maskNames)
    {
        if (profile is not null)
        {
            Console.WriteLine($"Specifying credentials profile '{profile}'");
            AWSConfigs.AWSProfileName = profile;
        }

        if (regionName is not null)
        {
            var region = RegionEndpoint.GetBySystemName(regionName);
            Console.WriteLine($"Specifying region {region.SystemName} ({region.DisplayName})");
            AWSConfigs.RegionEndpoint = region;
        }
    }


    int metricsReceived;

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var queueNames = await GetQueues(cancellationToken);

        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var cloudWatch = new AmazonCloudWatchClient();
        Console.WriteLine($"Loading CloudWatch metrics from {cloudWatch.Config.RegionEndpoint.SystemName}.");

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
            Console.Write($"\rGot data for {metricsReceived}/{queueNames.Length} SQS queues.");
        });

        await Task.WhenAll(tasks);

        // Clear out the last write of \r
        Console.WriteLine();

        return new QueueDetails
        {
            StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
            EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
            Queues = data.OrderBy(q => q.QueueName).ToArray(),
            TimeOfObservation = TimeSpan.FromDays(1)
        };
    }

    async Task<string[]> GetQueues(CancellationToken cancellationToken)
    {
        var sqs = new AmazonSQSClient();

        Console.WriteLine($"Loading SQS queue names from {sqs.Config.RegionEndpoint.SystemName}.");

        var request = new ListQueuesRequest
        {
            MaxResults = 1000
        };

        var queueNames = new List<string>();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await sqs.ListQueuesAsync(request, cancellationToken);

            queueNames.AddRange(response.QueueUrls.Select(url => url.Split('/')[4]));

            Console.Write($"\rFound {queueNames.Count} SQS queues.");

            if (response.NextToken is not null)
            {
                request.NextToken = response.NextToken;
            }
            else
            {
                Console.WriteLine();
                return queueNames.ToArray();
            }
        }
    }

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "AmazonSQS",
            ReportMethod = "AWS CloudWatch Metrics",
            SkipEndpointListCheck = true
        });
    }
}