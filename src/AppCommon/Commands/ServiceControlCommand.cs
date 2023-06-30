using System.CommandLine;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Particular.EndpointThroughputCounter.Data;
using Particular.EndpointThroughputCounter.Infra;
using Particular.EndpointThroughputCounter.ServiceControl;

partial class ServiceControlCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("servicecontrol", "Measure endpoints and throughput using the ServiceControl API");

        var scUrlArg = new Option<string>(
            name: PrimaryUrlArgName,
            description: "The URL for the primary ServiceControl instance API, http://localhost:33333/api by default")
        {
            IsRequired = true
        };

        var monitoringUrlArg = new Option<string>(
            name: MonitoringUrlArgName,
            description: "The URL for the ServiceControl Monitoring instance API, http://localhost:33633/ by default")
        {
            IsRequired = true
        };

        command.AddOption(scUrlArg);
        command.AddOption(monitoringUrlArg);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var scUrl = context.ParseResult.GetValueForOption(scUrlArg);
            var monUrl = context.ParseResult.GetValueForOption(monitoringUrlArg);
            var cancellationToken = context.GetCancellationToken();

            RunInfo.Add("ServiceControlUrl", scUrl);
            RunInfo.Add("MonitoringUrl", monUrl);

            var runner = new ServiceControlCommand(shared, scUrl, monUrl);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    const string PrimaryUrlArgName = "--serviceControlApiUrl";
    const string MonitoringUrlArgName = "--monitoringApiUrl";

    static readonly Version MinAuditCountsVersion = new Version(4, 29);

    readonly string primaryUrl;
    readonly string monitoringUrl;
    ServiceControlClient primary;
    ServiceControlClient monitoring;
    ServiceControlEndpoint[] knownEndpoints;

#if DEBUG
    // So that a run can be done in 3 minutes in debug mode
    const int SampleCount = 3;
    const int MinutesPerSample = 1;
#else
    const int SampleCount = 24;
    const int MinutesPerSample = 60;
#endif

    public ServiceControlCommand(SharedOptions shared, string primaryUrl, string monitoringUrl)
        : base(shared)
    {
        this.primaryUrl = primaryUrl;
        this.monitoringUrl = monitoringUrl;
    }

    protected override async Task Initialize(CancellationToken cancellationToken = default)
    {
        var httpFactory = await InteractiveHttpAuth.CreateHttpClientFactory(primaryUrl, configureNewClient: c => c.Timeout = TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);
        primary = new ServiceControlClient(PrimaryUrlArgName, "ServiceControl", primaryUrl, httpFactory);
        monitoring = new ServiceControlClient(MonitoringUrlArgName, "ServiceControl Monitoring", monitoringUrl, httpFactory);
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var allData = new List<QueueThroughput>();

        var start = DateTimeOffset.Now.AddMinutes(-MinutesPerSample);

        Out.WriteLine($"The tool will sample ServiceControl data {SampleCount} times at {MinutesPerSample}-minute intervals.");

        Out.WriteLine("Performing initial data sampling...");
        allData.AddRange(await SampleData(MinutesPerSample, cancellationToken));

        for (var i = 1; i < SampleCount; i++)
        {
            Out.WriteLine($"{i}/{SampleCount} samplings complete");
            DateTime waitUntilUtc = DateTime.UtcNow.AddMinutes(MinutesPerSample);

            await Out.CountdownTimer("Time until next sampling", waitUntilUtc, cancellationToken: cancellationToken);

            allData.AddRange(await SampleData(MinutesPerSample, cancellationToken));
        }
        Out.WriteLine("Sampling complete");

        var queues = allData.GroupBy(q => q.QueueName, StringComparer.OrdinalIgnoreCase)
            .Select(g => new QueueThroughput { QueueName = g.Key, Throughput = g.Sum(q => q.Throughput) })
            .ToList();

        var recordedEndpoints = queues.ToDictionary(q => q.QueueName, StringComparer.OrdinalIgnoreCase);

        foreach (var knownEndpoint in knownEndpoints)
        {
            var recordedByMetrics = recordedEndpoints.GetOrDefault(knownEndpoint.Name);
            if (knownEndpoint.AuditCounts?.Any() ?? false)
            {
                var highestAuditCount = knownEndpoint.AuditCounts.Max(ac => ac.Count);
                if (recordedByMetrics is not null)
                {
                    recordedByMetrics.Throughput = Math.Max(recordedByMetrics.Throughput ?? 0, highestAuditCount);
                }
                else
                {
                    queues.Add(new QueueThroughput { QueueName = knownEndpoint.Name, Throughput = highestAuditCount });
                }
            }
            else if (recordedByMetrics is null)
            {
                queues.Add(new QueueThroughput
                {
                    QueueName = knownEndpoint.Name,
                    NoDataOrSendOnly = true
                });
            }
        }

        var now = DateTimeOffset.Now;

        return new QueueDetails
        {
            StartTime = start,
            EndTime = now,
            Queues = queues.OrderBy(q => q.QueueName).ToArray()
        };
    }

    async Task<QueueThroughput[]> SampleData(int minutes, CancellationToken cancellationToken)
    {
        // Lots of retries here because we get multiple queues in one go
        var arr = await monitoring.GetData<JArray>($"/monitored-endpoints?history={minutes}", 5, cancellationToken);

        var queueResults = arr.Select(token =>
        {
            var name = token["name"].Value<string>();
            var throughputAvgPerSec = token["metrics"]["throughput"]["average"].Value<double>();
            var throughputTotal = throughputAvgPerSec * minutes * 60;

            return new QueueThroughput
            {
                QueueName = name,
                Throughput = (int)throughputTotal
            };
        }).ToList();

        var monitoredNames = queueResults.Select(ep => ep.QueueName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        var endpointsToCheckForHourlyAuditData = knownEndpoints.Where(ep => !monitoredNames.Contains(ep.Name) && ep.CheckHourlyAuditDataIfNoMonitoringData).ToArray();
        if (endpointsToCheckForHourlyAuditData.Any())
        {
            var auditsFromBinarySearch = new ServiceControlAuditsByBinarySearch(primary, MinutesPerSample);

            foreach (var endpoint in endpointsToCheckForHourlyAuditData)
            {
                var fromAuditing = await auditsFromBinarySearch.GetThroughputFromAudits(endpoint.Name, cancellationToken);
                if (fromAuditing is not null)
                {
                    queueResults.Add(fromAuditing);
                }
            }
        }

        return queueResults.ToArray();
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        await primary.CheckEndpoint(content => content.Contains("\"known_endpoints_url\"") && content.Contains("\"endpoints_messages_url\""), cancellationToken);
        await monitoring.CheckEndpoint(content => content.Contains("\"instanceType\"") && content.Contains("\"monitoring\""), cancellationToken);

        knownEndpoints = await GetKnownEndpoints(cancellationToken);

        if (!knownEndpoints.Any())
        {
            throw new HaltException(HaltReason.InvalidEnvironment, "Successfully connected to ServiceControl API but no known endpoints could be found. Are you using the correct URL?");
        }

        // Tool can't proceed without this data, try 5 times
        var obj = await primary.GetData<JObject>("/configuration", 5, cancellationToken);

        var transportTypeToken = obj["transport"]["transport_customization_type"]
                                 ?? obj["transport"]["transport_type"]
                                 ?? throw new HaltException(HaltReason.InvalidEnvironment, "This version of ServiceControl is not supported. Update to a supported version of ServiceControl. See https://docs.particular.net/servicecontrol/upgrades/supported-versions");

        var transportCustomizationTypeStr = transportTypeToken.Value<string>();

        var split = transportCustomizationTypeStr.Split(',');

        var classNameSplit = split[0].Split('.');

        var className = classNameSplit.Last().Trim();

        const string postfix = "TransportCustomization";

        if (className.EndsWith(postfix))
        {
            className = className[..^postfix.Length];
        }

        return new EnvironmentDetails
        {
            MessageTransport = className,
            ReportMethod = "ServiceControl API",
            QueueNames = knownEndpoints.OrderBy(q => q.Name).Select(q => q.Name).ToArray(),
            QueuesAreEndpoints = true
        };
    }

    async Task<ServiceControlEndpoint[]> GetKnownEndpoints(CancellationToken cancellationToken)
    {
        // Tool can't proceed without this data, try 5 times
        var arr = await primary.GetData<JArray>("/endpoints", 5, cancellationToken);

        var endpoints = arr.Select(endpointToken => new
        {
            Name = endpointToken["name"].Value<string>(),
            HeartbeatsEnabled = endpointToken["monitored"].Value<bool>(),
            ReceivingHeartbeats = endpointToken["heartbeat_information"]["reported_status"].Value<string>() == "beating"
        })
        .GroupBy(x => x.Name)
        .Select(g => new ServiceControlEndpoint
        {
            Name = g.Key,
            HeartbeatsEnabled = g.Any(e => e.HeartbeatsEnabled),
            ReceivingHeartbeats = g.Any(e => e.ReceivingHeartbeats)
        })
        .ToArray();

        var useAuditCounts = false;

        if (primary.Version.Version >= MinAuditCountsVersion)
        {
            // Verify audit instances also have audit counts
            var remotesInfoJson = await primary.GetData<JArray>("/configuration/remotes", cancellationToken);
            var remoteInfo = remotesInfoJson.Select(remote =>
            {
                var uri = remote["api_uri"].Value<string>();
                var status = remote["status"].Value<string>();
                var versionString = remote["version"]?.Value<string>();
                var retentionString = remote["configuration"]?["data_retention"]?["audit_retention_period"]?.Value<string>();

                return new
                {
                    Uri = uri,
                    Status = status,
                    VersionString = versionString,
                    SemVer = SemVerVersion.TryParse(versionString, out var v) ? v : null,
                    Retention = TimeSpan.TryParse(retentionString, out var ts) ? ts : TimeSpan.Zero
                };
            })
            .ToArray();

            foreach (var remote in remoteInfo)
            {
                if (remote.Status == "online" || remote.SemVer is not null)
                {
                    Out.WriteLine($"ServiceControl Audit instance at {remote.Uri} detected running version {remote.SemVer}");
                }
                else
                {
                    var configUrl = primary.GetFullUrl("/configuration/remotes");
                    var remoteConfigMsg = $"Unable to determine the version of one or more ServiceControl Audit instances. For the instance with URI {remote.Uri}, the status was '{remote.Status}' and the version string returned was '{remote.VersionString}'. If you are not able to resolve this issue on your own, send the contents of {configUrl} to Particular when requesting help.";
                    throw new HaltException(HaltReason.InvalidEnvironment, remoteConfigMsg);
                }
            }

            // Want 2d audit retention so we get one complete UTC day no matter what time it is
            useAuditCounts = remoteInfo.All(r => r.SemVer.Version >= MinAuditCountsVersion && r.Retention > TimeSpan.FromDays(2));
        }

        foreach (var endpoint in endpoints)
        {
            if (useAuditCounts)
            {
                var path = $"/endpoints/{endpoint.Name}/audit-count";
                endpoint.AuditCounts = await primary.GetData<AuditCount[]>(path, 2, cancellationToken);
                endpoint.CheckHourlyAuditDataIfNoMonitoringData = false;
            }
            else
            {
                var path = $"/endpoints/{endpoint.Name}/messages/?per_page=1";
                var recentMessages = await primary.GetData<JArray>(path, 2, cancellationToken);
                endpoint.CheckHourlyAuditDataIfNoMonitoringData = recentMessages.Any();
            }
        }

        return endpoints;
    }

    class ServiceControlEndpoint
    {
        public string Name { get; set; }
        public bool HeartbeatsEnabled { get; set; }
        public bool ReceivingHeartbeats { get; set; }
        public bool CheckHourlyAuditDataIfNoMonitoringData { get; set; }
        public AuditCount[] AuditCounts { get; set; } = Array.Empty<AuditCount>();
    }

    class AuditCount
    {
        [JsonProperty("utc_date")]
        public DateTime UtcDate { get; set; }
        [JsonProperty("count")]
        public long Count { get; set; }
    }
}