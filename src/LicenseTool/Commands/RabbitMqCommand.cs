using System.CommandLine;
using LicenseTool.Data;

class RabbitMqCommand : BaseSamplingCommand<List<RabbitQueueDetails>>
{
    public static Command CreateCommand()
    {
        var command = new Command("rabbitmq", "Measure endpoints and throughput using the RabbitMQ management API");

        var urlArg = new Option<string>(
            name: "--apiUrl",
            description: "The RabbitMQ Management API URL");

        var userArg = new Option<string>(
            name: "--username",
            description: "The username for the RabbitMQ Management API");

        var passArg = new Option<string>(
            name: "--password",
            description: "The username for the RabbitMQ Management API");

        command.AddOption(urlArg);
        command.AddOption(userArg);
        command.AddOption(passArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        command.AddOption(maskNames);

        command.SetHandler(async (url, user, pass, maskNames) =>
        {
            var rabbitManagement = new RabbitManagement(url, user, pass);
            var runner = new RabbitMqCommand(rabbitManagement);
            await runner.Run(maskNames, CancellationToken.None);
        },
        urlArg, userArg, passArg, maskNames);

        return command;
    }

    RabbitManagement rabbit;

    public RabbitMqCommand(RabbitManagement rabbit)
    {
        this.rabbit = rabbit;
    }

    protected override Task<List<RabbitQueueDetails>> SampleData(CancellationToken cancellationToken = default)
        => rabbit.GetThroughput(cancellationToken);

    protected override IEnumerable<QueueThroughput> CalculateThroughput(List<RabbitQueueDetails> start, List<RabbitQueueDetails> end)
    {
        var queueThroughputs = new List<QueueThroughput>();
        var endDict = end.ToDictionary(q => q.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var queue in start)
        {
            if (queue.Name.StartsWith("nsb.delay-level-") || queue.Name.StartsWith("nsb.v2.delay-level-"))
            {
                continue;
            }

            if (queue.Name is "error" or "audit")
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
}

