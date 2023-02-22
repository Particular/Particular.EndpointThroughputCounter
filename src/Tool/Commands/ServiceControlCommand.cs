using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

    readonly ServiceControlClient primary;
    readonly ServiceControlClient monitoring;
    ServiceControlEndpoint[] knownEndpoints;

#if DEBUG
    // So that a run can be done in 3 minutes in debug mode
    const int SampleCount = 3;
    const int MinutesPerSample = 1;
    const int AuditSamplingPageSize = 5;
#else
    const int SampleCount = 24;
    const int MinutesPerSample = 60;
    const int AuditSamplingPageSize = 500;
#endif

    public ServiceControlCommand(SharedOptions shared, string primaryUrl, string monitoringUrl)
        : base(shared)
    {
        primary = new ServiceControlClient(PrimaryUrlArgName, "ServiceControl", primaryUrl);
        monitoring = new ServiceControlClient(MonitoringUrlArgName, "ServiceControl Monitoring", monitoringUrl);
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

        var queues = allData.GroupBy(q => q.QueueName)
            .Select(g => new QueueThroughput { QueueName = g.Key, Throughput = g.Sum(q => q.Throughput) })
            .ToList();

        var recordedEndpoints = queues.Select(q => q.QueueName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var knownEndpoint in knownEndpoints)
        {
            if (!recordedEndpoints.Contains(knownEndpoint.Name))
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

        foreach (var endpoint in knownEndpoints.Where(ep => !monitoredNames.Contains(ep.Name) && ep.AuditedMessages))
        {
            var fromAuditing = await GetThroughputFromAudits(endpoint.Name, cancellationToken);
            if (fromAuditing is not null)
            {
                queueResults.Add(fromAuditing);
            }
        }

        return queueResults.ToArray();
    }

    async Task<QueueThroughput> GetThroughputFromAudits(string endpointName, CancellationToken cancellationToken)
    {
        Out.WriteLine($"Getting throughput from {endpointName} using audit data.");

        try
        {
            return await GetThroughputFromAuditsInternal(endpointName, cancellationToken);
        }
        catch (ServiceControlDataException x)
        {
            Out.WriteError($"Warning: Unable to read ServiceControl data from {x.Url} after {x.Attempts} attempts: {x.Message}");
            return null;
        }
    }

    async Task<QueueThroughput> GetThroughputFromAuditsInternal(string endpointName, CancellationToken cancellationToken)
    {
        var collectionPeriodStartTime = DateTime.UtcNow.AddMinutes(-MinutesPerSample);

        async Task<AuditBatch> GetPage(int page)
        {
            Debug($"  * Getting page {page}");
            return await GetAuditBatch(endpointName, page, AuditSamplingPageSize, cancellationToken);
        }

        var firstPage = await GetPage(1);

        if (!firstPage.IsValid)
        {
            return null;
        }

        if (firstPage.ContainsTime(collectionPeriodStartTime))
        {
            return new QueueThroughput { QueueName = endpointName, Throughput = firstPage.MessagesProcessedAfter(collectionPeriodStartTime) };
        }

        // Goal: arrive with a minimum and maximum page where the collectionPeriodStartTime occurs somewhere in the middle,
        // and at that point we can start a binary search to find the exact page in the middle contianing that timestamp.
        // Start with minimum page is one (duh) and maximum page is currently unknown, use -1 to represent that.
        var minPage = 1;
        var maxPage = -1;

        // First we need to "guess" which page the collectionPeriodStartTime might exist on based on the time it took to
        // process the messages that exist on page 1.
        var estimatedMessagesThisSample = TimeSpan.FromMinutes(MinutesPerSample).TotalSeconds / firstPage.AverageSecondsPerMessage;
        // Make our educated guess 120% of what the math-based extrapolation tells us so that if the real math estimate is almost exactly right,
        // the page is ensured to be in the first range for the binary search. This also saves us from double-to-int conversion slicing off
        // the estimate resulting in the true page being just outside the first min-max page range causing us to have to go to the next range.
        var estimatedPages = 1.2 * estimatedMessagesThisSample / AuditSamplingPageSize;
        Debug($"  * Estimating {estimatedPages:0.0} pages");

        // This is not a "normal" for loop because we're not using the same variable in each of the 3 segments.
        // 1. Start with factor = 1, this expresses a hope that our "guess" from above is accurate
        // 2. We continue as long as maxPage is set to an actual (positive) number, this is not a "factor < N" situation.
        //    So this loop is more like a while (maxPage == -1) than a for loop but we have our iteration of factor built in.
        // 3. Each time the loop runs, we increase the factor - we didn't find the range so we need to try the next range
        for (var factor = 1; maxPage == -1; factor++)
        {
            var attemptPageNum = (int)(factor * estimatedPages);
            var page = await GetPage(attemptPageNum);

            if (page.ContainsTime(collectionPeriodStartTime))
            {
                return new QueueThroughput
                {
                    QueueName = endpointName,
                    Throughput = (AuditSamplingPageSize * (attemptPageNum - 1)) + page.MessagesProcessedAfter(collectionPeriodStartTime)
                };
            }

            if (page.DataIsBefore(collectionPeriodStartTime))
            {
                // Either we got past the retention period of data, or we're past the sample period
                // Which means it's time to assign the max page and start the binary search
                maxPage = attemptPageNum;
            }
            else
            {
                // We already know we haven't gone far enough, no reason to re-examine
                // pages lower than this when doing the binary search.
                minPage = attemptPageNum;
            }
        }

        Debug($"  * Starting binary search with min {minPage}, max {maxPage}");
        // Do a binary search to find the page that represents where 1 hour ago was
        while (minPage != maxPage)
        {
            var middlePageNum = (minPage + maxPage) / 2;
            var pageData = await GetPage(middlePageNum);

            // If we've backtracked to a page we've hit before, or the page actually contains the time we seek, we're done
            if (middlePageNum == minPage || middlePageNum == maxPage || pageData.ContainsTime(collectionPeriodStartTime))
            {
                Debug($"  * Found => {(AuditSamplingPageSize * (middlePageNum - 1)) + pageData.MessagesProcessedAfter(collectionPeriodStartTime)} messages");
                return new QueueThroughput
                {
                    QueueName = endpointName,
                    Throughput = (AuditSamplingPageSize * (middlePageNum - 1)) + pageData.MessagesProcessedAfter(collectionPeriodStartTime)
                };
            }

            if (pageData.DataIsBefore(collectionPeriodStartTime))
            {
                // Went too far, cut out the top half
                maxPage = middlePageNum;
            }
            else if (pageData.DataIsAfter(collectionPeriodStartTime))
            {
                // Not far enough, cut out the bottom half
                minPage = middlePageNum;
            }
        }

        // Likely we don't get here, but for completeness
        var finalPage = await GetPage(minPage);
        Debug($"  * Catch-All => {(AuditSamplingPageSize * (minPage - 1)) + finalPage.MessagesProcessedAfter(collectionPeriodStartTime)} messages");
        return new QueueThroughput
        {
            QueueName = endpointName,
            Throughput = (AuditSamplingPageSize * (minPage - 1)) + finalPage.MessagesProcessedAfter(collectionPeriodStartTime)
        };
    }

    async Task<AuditBatch> GetAuditBatch(string endpointName, int page, int pageSize, CancellationToken cancellationToken)
    {
        var pathAndQuery = $"/endpoints/{endpointName}/messages/?page={page}&per_page={pageSize}&sort=processed_at&direction=desc";

        var arr = await primary.GetData<JArray>(pathAndQuery, cancellationToken);

        var processedAtValues = arr.Select(token => token["processed_at"].Value<DateTime>()).ToArray();

        return new AuditBatch(processedAtValues);
    }

    record struct AuditBatch
    {
        public AuditBatch(DateTime[] timestamps)
        {
            this.timestamps = timestamps;

            IsValid = timestamps.Length > 0;

            if (IsValid)
            {
                firstMessageProcessedAt = timestamps.Min();
                lastMessageProcessedAt = timestamps.Max();
                AverageSecondsPerMessage = (lastMessageProcessedAt - firstMessageProcessedAt).TotalSeconds / timestamps.Length;
            }
            else
            {
                firstMessageProcessedAt = default;
                lastMessageProcessedAt = default;
                AverageSecondsPerMessage = 0;
            }
        }

        DateTime[] timestamps;
        DateTime firstMessageProcessedAt;
        DateTime lastMessageProcessedAt;

        public double AverageSecondsPerMessage { get; }
        public bool IsValid { get; }

        public bool ContainsTime(DateTime targetTime)
        {
            return timestamps.Length > 0 && firstMessageProcessedAt <= targetTime && targetTime <= lastMessageProcessedAt;
        }

        public bool DataIsBefore(DateTime cutoff)
        {
            return timestamps.Length == 0 || lastMessageProcessedAt < cutoff;
        }

        public bool DataIsAfter(DateTime cutoff)
        {
            return timestamps.Length > 0 && firstMessageProcessedAt > cutoff;
        }

        public int MessagesProcessedAfter(DateTime cutoff)
        {
            return timestamps.Count(dt => dt >= cutoff);
        }
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
            ?? throw new HaltException(HaltReason.InvalidEnvironment, "This version of ServiceControl is not supported. Update to a supported version of ServiceControl. See https://docs.particular.net/servicecontrol/upgrades/supported-versions");

        var transportCustomizationTypeStr = obj["transport"]["transport_customization_type"].Value<string>();

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
            QueueNames = knownEndpoints.OrderBy(q => q.Name).Select(q => q.Name).ToArray()
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

        foreach (var endpoint in endpoints)
        {
            var messagesPath = $"/endpoints/{endpoint.Name}/messages/?per_page=1";

            var recentMessages = await primary.GetData<JArray>(messagesPath, 2, cancellationToken);

            endpoint.AuditedMessages = recentMessages.Any();
        }

        return endpoints;
    }

    [Conditional("DEBUG")]
    void Debug(string message)
    {
        Out.WriteLine(message);
    }

    class ServiceControlEndpoint
    {
        public string Name { get; set; }
        public bool HeartbeatsEnabled { get; set; }
        public bool ReceivingHeartbeats { get; set; }
        public bool AuditedMessages { get; set; }
    }
}