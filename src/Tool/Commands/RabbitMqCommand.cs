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

        command.SetHandler(async context =>
        {
            var url = context.ParseResult.GetValueForOption(urlArg);
            var maskNames = context.ParseResult.GetValueForOption(SharedOptions.MaskNames);
            var cancellationToken = context.GetCancellationToken();

            var rabbitManagement = new RabbitManagement(url);
            var runner = new RabbitMqCommand(rabbitManagement, maskNames);

            await runner.Run(cancellationToken);
        });

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

        foreach (var queue in start.Where(q => IncludeQueue(q.Name)))
        {
            if (endDict.TryGetValue(queue.Name, out var queueEnd))
            {
                queueThroughputs.Add(new QueueThroughput { QueueName = queue.Name, Throughput = queueEnd.AckedMessages - queue.AckedMessages });
            }
        }

        return queueThroughputs;
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        var queueNames = (await rabbit.GetQueueDetails(cancellationToken))
            .Where(q => IncludeQueue(q.Name))
            .OrderBy(q => q.Name)
            .Select(q => q.Name)
            .ToArray();

        return new EnvironmentDetails
        {
            MessageTransport = "RabbitMQ",
            ReportMethod = "ThroughputTool: RabbitMQ Admin",
            QueueNames = queueNames
        };
    }

    static bool IncludeQueue(string name)
    {
        if (name.StartsWith("nsb.delay-level-") || name.StartsWith("nsb.v2.delay-level-") || name.StartsWith("nsb.v2.verify-"))
        {
            return false;
        }
        if (name is "error" or "audit")
        {
            return false;
        }

        if (name.StartsWith("Particular.", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }
}

