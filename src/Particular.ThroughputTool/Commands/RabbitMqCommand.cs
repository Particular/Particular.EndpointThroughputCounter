using System.CommandLine;
using Particular.ThroughputTool.Data;

class RabbitMqCommand : BaseSamplingCommand<List<RabbitQueueDetails>>
{
    public static Command CreateCommand()
    {
        var command = new Command("rabbitmq", "Measure endpoints and throughput using the RabbitMQ management API");

        var urlArg = new Option<string>(
            name: "--apiUrl",
            description: "The RabbitMQ Management API URL");

        command.AddOption(urlArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        var outputPath = SharedOptions.CreateOutputPathOption();
        command.AddOption(maskNames);
        command.AddOption(outputPath);


        command.SetHandler(async (url, outputPath, maskNames) =>
        {
            var rabbitManagement = new RabbitManagement(url);
            var runner = new RabbitMqCommand(rabbitManagement, outputPath, maskNames);
            await runner.Run(CancellationToken.None);
        },
        urlArg, outputPath, maskNames);

        return command;
    }

    readonly RabbitManagement rabbit;

    public RabbitMqCommand(RabbitManagement rabbit, string outputPath, string[] maskNames)
        : base(outputPath, maskNames) => this.rabbit = rabbit;

    protected override Task<List<RabbitQueueDetails>> SampleData(CancellationToken cancellationToken = default)
        => rabbit.GetThroughput(cancellationToken);

    protected override IEnumerable<QueueThroughput> CalculateThroughput(List<RabbitQueueDetails> start, List<RabbitQueueDetails> end)
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

