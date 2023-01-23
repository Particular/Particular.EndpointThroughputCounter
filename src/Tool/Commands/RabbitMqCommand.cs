using System;
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
            var shared = SharedOptions.Parse(context);
            var cancellationToken = context.GetCancellationToken();

            var rabbitManagement = new RabbitManagement(url);
            var runner = new RabbitMqCommand(shared, rabbitManagement);

            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly RabbitManagement rabbit;
    readonly TimeSpan pollingInterval;

    RabbitDetails rabbitDetails;

    public RabbitMqCommand(SharedOptions shared, RabbitManagement rabbit)
        : base(shared)
    {
        this.rabbit = rabbit;
#if DEBUG
        pollingInterval = TimeSpan.FromSeconds(10);
#else
        pollingInterval = TimeSpan.FromMinutes(5);
#endif
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Out.WriteLine("Taking initial queue statistics.");
        var startData = await rabbit.GetQueueDetails(cancellationToken);
        var startTime = DateTimeOffset.Now;

        if (startData.All(q => q.AckedMessages is null))
        {
            throw new HaltException(HaltReason.InvalidEnvironment, $"None of the queues at {rabbit.ManagementUri}/api/queues is reporting any message_stats elements. Are you sure the system is actively processing messages and is configured to track queue statistics?");
        }

        var trackers = startData
            .Where(start => IncludeQueue(start.Name))
            // RabbitMQ queue names are case sensitive and SOMEHOW sometimes we see duplicates anyway
            .GroupBy(start => start.Name, StringComparer.InvariantCulture)
            .ToDictionary(g => g.Key, g => new QueueTracker(g.First()), StringComparer.InvariantCulture);
        var nextPollTime = DateTime.UtcNow + pollingInterval;

        async Task UpdateTrackers()
        {
            var data = await rabbit.GetQueueDetails(cancellationToken);
            foreach (var q in data)
            {
                if (trackers.TryGetValue(q.Name, out var tracker))
                {
                    tracker.AddData(q);
                }
            }
            nextPollTime = DateTime.UtcNow + pollingInterval;
        }

        Out.WriteLine("Waiting until next reading...");
        var waitUntil = DateTime.UtcNow + PollingRunTime;

        await Out.CountdownTimer("Data Collection Time Left", waitUntil, cancellationToken: cancellationToken, onLoopAction: async () =>
        {
            if (DateTime.UtcNow > nextPollTime)
            {
                await UpdateTrackers();
            }
        });

        Out.WriteLine();
        Out.WriteLine("Taking final queue statistics.");
        await UpdateTrackers();
        var endTime = DateTimeOffset.Now;

        var queues = trackers.Values
            .Select(t => new QueueThroughput
            {
                QueueName = t.Name,
                Throughput = t.AckedMessages
            })
            .OrderBy(q => q.QueueName)
            .ToArray();

        return new QueueDetails
        {
            Queues = queues,
            StartTime = startTime,
            EndTime = endTime
        };
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        rabbitDetails = await rabbit.GetRabbitDetails(cancellationToken);

        Out.WriteLine($"Connected to cluster {rabbitDetails.ClusterName}");
        Out.WriteLine($"  - RabbitMQ Version: {rabbitDetails.RabbitVersion}");
        Out.WriteLine($"  - Management Plugin Version: {rabbitDetails.ManagementVersion}");

        var queueNames = (await rabbit.GetQueueDetails(cancellationToken))
            .Where(q => IncludeQueue(q.Name))
            .OrderBy(q => q.Name)
            .Select(q => q.Name)
            .ToArray();

        return new EnvironmentDetails
        {
            MessageTransport = "RabbitMQ",
            ReportMethod = rabbitDetails.ToReportMethodString(),
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

    class QueueTracker
    {
        public QueueTracker(RabbitQueueDetails startReading)
        {
            Name = startReading.Name;
            Baseline = startReading.AckedMessages ?? 0;
            AckedMessages = 0;
        }

        public string Name { get; init; }
        public int Baseline { get; private set; }
        public int AckedMessages { get; private set; }

        public void AddData(RabbitQueueDetails newReading)
        {
            if (newReading.AckedMessages is not null)
            {
                if (newReading.AckedMessages.Value >= Baseline)
                {
                    var newlyAckedMessages = newReading.AckedMessages.Value - Baseline;
                    AckedMessages += newlyAckedMessages;
                }
                Baseline = newReading.AckedMessages.Value;
            }
        }
    }
}

