﻿using System.CommandLine;
using System.Net;
using Particular.EndpointThroughputCounter.Infra;
using Particular.LicensingComponent.Report;
using Particular.ThroughputQuery;
using Particular.ThroughputQuery.RabbitMQ;

class RabbitMqCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("rabbitmq", "Measure endpoints and throughput using the RabbitMQ management API");

        var urlArg = new Option<string>(
            name: "--apiUrl",
            description: "The RabbitMQ Management API URL")
        {
            IsRequired = true
        };

        command.AddOption(urlArg);

        command.SetHandler(async context =>
        {
            var url = context.ParseResult.GetValueForOption(urlArg);
            var shared = SharedOptions.Parse(context);
            var cancellationToken = context.GetCancellationToken();

            RunInfo.Add("RabbitMQUrl", url);

            var runner = new RabbitMqCommand(shared, url);

            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly string managementUrl;
    readonly TimeSpan pollingInterval;
    RabbitMQManagementClient _rabbitMQ;
    RabbitMQDetails _rabbitMQDetails;

    public RabbitMqCommand(SharedOptions shared, string managementUrl)
        : base(shared)
    {
        this.managementUrl = managementUrl;
#if DEBUG
        pollingInterval = TimeSpan.FromSeconds(10);
#else
        pollingInterval = TimeSpan.FromMinutes(5);
#endif
    }

    protected override async Task Initialize(CancellationToken cancellationToken = default)
    {
        var defaultCredential = new NetworkCredential("guest", "guest");
        var httpFactory = await InteractiveHttpAuth.CreateHttpClientFactory(managementUrl.TrimEnd('/') + "/api/overview", defaultCredential: defaultCredential, cancellationToken: cancellationToken);

        _rabbitMQ = new RabbitMQManagementClient(httpFactory, managementUrl);
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Out.WriteLine("Taking initial queue statistics.");
        var startData = await _rabbitMQ.GetQueueDetails(cancellationToken);
        await _rabbitMQ.AddAdditionalQueueDetails(startData, cancellationToken);
        var startTime = DateTimeOffset.Now;

        if (startData.All(q => q.AckedMessages is null))
        {
            throw new HaltException(HaltReason.InvalidEnvironment, $"None of the queues at {_rabbitMQ.ManagementUri}/api/queues is reporting any message_stats elements. Are you sure the system is actively processing messages and is configured to track queue statistics?");
        }

        var trackers = startData
            .Where(start => IncludeQueue(start.Name))
            .Select(start => new QueueTracker(start))
            .ToDictionary(t => t.Id, StringComparer.InvariantCulture);
        var nextPollTime = DateTime.UtcNow + pollingInterval;

        async Task UpdateTrackers()
        {
            var data = await _rabbitMQ.GetQueueDetails(cancellationToken);
            foreach (var q in data)
            {
                if (trackers.TryGetValue(q.Id, out var tracker))
                {
                    tracker.AddData(q);
                }
            }
        }

        Out.WriteLine("Waiting until next reading...");
        var waitUntil = DateTime.UtcNow + PollingRunTime;

        var failCount = 0;

        await Out.CountdownTimer("Data Collection Time Left", waitUntil, cancellationToken: cancellationToken, onLoopAction: () =>
        {
            if (DateTime.UtcNow > nextPollTime)
            {
                // Set this immediately, as loop may swing around in only 250ms
                nextPollTime = DateTime.UtcNow + pollingInterval;

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await UpdateTrackers();
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception x)
                    {
                        failCount++;
                        if (failCount >= 15)
                        {
                            // 1 day is 1440m which is 96 five-minute reporting intervals. 15 failures is 15.6% which is over an (arbitrary) 5% failure rate
                            // indicating it's probably better to stop the collection rather than continue collecting untrustworthy data.
                            throw new HaltException(HaltReason.RuntimeError, "The connection to RabbitMQ has failed too many times and appears unreliable.", x);
                        }
                        Out.WriteLine();
                        Out.WriteWarn($"Encountered error updating statistics, ignoring for now: {x.Message}");
                        Out.WriteDebugTimestamp();
                    }
                });
            }
        });

        Out.WriteLine();
        Out.WriteLine("Taking final queue statistics.");
        await UpdateTrackers();
        var endTime = DateTimeOffset.Now;

        var useScopes = trackers.Values.Select(t => t.VHost).Distinct().Count() > 1;

        var queues = trackers.Values
            .Select(t => new QueueThroughput
            {
                Scope = useScopes ? t.VHost : null,
                QueueName = t.Name,
                Throughput = t.AckedMessages,
                EndpointIndicators = t.EndpointIndicators.Any() ? t.EndpointIndicators : null

            })
            .OrderBy(q => q.Scope)
                .ThenBy(q => q.QueueName)
            .ToArray();

        return new QueueDetails
        {
            ScopeType = useScopes ? "VirtualHost" : null,
            Queues = queues,
            StartTime = startTime,
            EndTime = endTime
        };
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        try
        {
            _rabbitMQDetails = await _rabbitMQ.GetRabbitDetails(cancellationToken);

            Out.WriteLine($"Connected to cluster {_rabbitMQDetails.ClusterName}");
            Out.WriteLine($"  - RabbitMQ Version: {_rabbitMQDetails.RabbitMQVersion}");
            Out.WriteLine($"  - Management Plugin Version: {_rabbitMQDetails.ManagementVersion}");

            var queueNames = (await _rabbitMQ.GetQueueDetails(cancellationToken))
                .Where(q => IncludeQueue(q.Name))
                .Select(q => q.Name)
                .Distinct()
                .OrderBy(name => name)
                .ToArray();

            return new EnvironmentDetails
            {
                MessageTransport = "RabbitMQ",
                ReportMethod = _rabbitMQDetails.ToReportMethodString(),
                QueueNames = queueNames
            };
        }
        catch (QueryException x)
        {
            throw new HaltException(x);
        }
    }

    static bool IncludeQueue(string name)
    {
        if (name.StartsWith("nsb.delay-level-") || name.StartsWith("nsb.v2.delay-level-") || name.StartsWith("nsb.v2.verify-"))
        {
            return false;
        }

        return true;
    }

    class QueueTracker
    {
        public QueueTracker(RabbitMQQueueDetails startReading)
        {
            Id = startReading.Id;
            VHost = startReading.VHost;
            Name = startReading.Name;
            Baseline = startReading.AckedMessages ?? 0;
            AckedMessages = 0;
            EndpointIndicators = startReading.EndpointIndicators.ToArray();
        }

        public string Id { get; }
        public string VHost { get; }
        public string Name { get; }
        public long Baseline { get; private set; }
        public long AckedMessages { get; private set; }
        public string[] EndpointIndicators { get; init; }

        public void AddData(RabbitMQQueueDetails newReading)
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

