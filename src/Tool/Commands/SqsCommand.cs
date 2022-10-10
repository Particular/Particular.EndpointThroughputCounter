using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Particular.EndpointThroughputCounter.Data;

class SqsCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("sqs", "Measure endpoints and throughput using CloudWatch metrics for Amazon SQS");

        //var resourceIdArg = new Option<string>(
        //    name: "--resourceId",
        //    description: "The resource id for the Azure Service Bus namespace, which can be found in the Properties page in the Azure Portal.");

        //command.AddOption(resourceIdArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        command.AddOption(maskNames);

        command.SetHandler(async (maskNames) =>
        {
            var runner = new SqsCommand(maskNames);
            await runner.Run(CancellationToken.None);
        },
        maskNames);

        return command;
    }

    public SqsCommand(string[] maskNames)
    : base(maskNames)
    {
    }

    int metricsReceived;

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var credentials = GetCredentials();

        var cloudWatch = new AmazonCloudWatchClient(credentials, RegionEndpoint.USEast1);

        var data = new ConcurrentBag<QueueThroughput>();

        var queueNames = await GetQueues(credentials, cancellationToken);

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

        Console.WriteLine();

        return new QueueDetails
        {
            StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
            EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
            Queues = data.OrderBy(q => q.QueueName).ToArray(),
            TimeOfObservation = TimeSpan.FromDays(1)
        };
    }

    async Task<string[]> GetQueues(AWSCredentials credentials, CancellationToken cancellationToken)
    {
        var sqs = new AmazonSQSClient(credentials, RegionEndpoint.USEast1);

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

    AWSCredentials GetCredentials()
    {
        var accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");

        return new BasicAWSCredentials(accessKey, secretKey);
    }
}