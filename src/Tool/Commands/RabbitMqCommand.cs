using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Particular.EndpointThroughputCounter.Data;

class RabbitMqCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("rabbitmq", "Measure endpoints and throughput using the RabbitMQ management API");

        var urlArg = new Option<string>(
            name: "--apiUrl",
            description: "The RabbitMQ Management API URL");

        command.AddOption(urlArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        command.AddOption(maskNames);


        command.SetHandler(async (url, maskNames) =>
        {
            var rabbitManagement = new RabbitManagement(url);
            var runner = new RabbitMqCommand(rabbitManagement, maskNames);
            await runner.Run(CancellationToken.None);
        },
        urlArg, maskNames);

        return command;
    }

    readonly RabbitManagement rabbit;

    public RabbitMqCommand(RabbitManagement rabbit, string[] maskNames)
        : base(maskNames) => this.rabbit = rabbit;

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Taking initial queue statistics.");
        var startData = await rabbit.GetQueueDetails(cancellationToken);
        var startTime = DateTimeOffset.Now;

        Console.WriteLine("Waiting until next reading...");
        var waitUntil = DateTime.UtcNow + PollingRunTime;
        while (DateTime.UtcNow < waitUntil)
        {
            var timeLeft = waitUntil - DateTime.UtcNow;
            Console.Write($"\rWait Time Left: {timeLeft:hh':'mm':'ss}");
            await Task.Delay(250, cancellationToken);
        }

        Console.WriteLine();
        Console.WriteLine();

        Console.WriteLine("Taking final queue statistics.");
        var endData = await rabbit.GetQueueDetails(cancellationToken);
        var endTime = DateTimeOffset.Now;

        var queues = CalculateThroughput(startData, endData)
            .OrderBy(q => q.QueueName)
            .ToArray();

        return new QueueDetails
        {
            Queues = queues,
            StartTime = startTime,
            EndTime = endTime
        };
    }

    protected IEnumerable<QueueThroughput> CalculateThroughput(List<RabbitQueueDetails> start, List<RabbitQueueDetails> end)
    {
        var queueThroughputs = new List<QueueThroughput>();
        var endDict = end.ToDictionary(q => q.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var queue in start)
        {
            if (queue.Name.StartsWith("nsb.delay-level-") || queue.Name.StartsWith("nsb.v2.delay-level-") || queue.Name.StartsWith("nsb.v2.verify-"))
            {
                continue;
            }

            if (queue.Name is "error" or "audit")
            {
                continue;
            }

            if (queue.Name.StartsWith("Particular.", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (endDict.TryGetValue(queue.Name, out var queueEnd))
            {
                queueThroughputs.Add(new QueueThroughput { QueueName = queue.Name, Throughput = queueEnd.AckedMessages - queue.AckedMessages });
            }
        }

        return queueThroughputs;
    }

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
        => Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "RabbitMQ",
            ReportMethod = "ThroughputTool: RabbitMQ Admin"
        });
}

