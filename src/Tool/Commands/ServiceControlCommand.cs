using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Particular.EndpointThroughputCounter.Data;

partial class ServiceControlCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("servicecontrol", "Measure endpoints and throughput using the ServiceControl API");

        var scUrlArg = new Option<string>(
            name: "--serviceControlApiUrl",
            description: "The URL for the primary ServiceControl instance API, http://localhost:33333/api by default");

        var monitoringUrlArg = new Option<string>(
            name: "--monitoringApiUrl",
            description: "The URL for the ServiceControl Monitoring instance API, http://localhost:33633/ by default");

        command.AddOption(scUrlArg);
        command.AddOption(monitoringUrlArg);

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var scUrl = context.ParseResult.GetValueForOption(scUrlArg);
            var monUrl = context.ParseResult.GetValueForOption(monitoringUrlArg);
            var cancellationToken = context.GetCancellationToken();

            var runner = new ServiceControlCommand(shared, scUrl, monUrl);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly AuthenticatingHttpClient http;
    readonly JsonSerializer serializer;
    readonly string primaryUrl;
    readonly string monitoringUrl;
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
        this.primaryUrl = primaryUrl.TrimEnd('/');
        this.monitoringUrl = monitoringUrl.TrimEnd('/');

        http = new AuthenticatingHttpClient(client => client.Timeout = TimeSpan.FromSeconds(10));
        serializer = new JsonSerializer();
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var allData = new List<QueueThroughput>();

        var start = DateTimeOffset.Now.AddMinutes(-MinutesPerSample);

        Console.WriteLine($"The tool will sample ServiceControl data {SampleCount} times at {MinutesPerSample}-minute intervals.");

        Console.WriteLine("Performing initial data sampling...");
        allData.AddRange(await SampleData(MinutesPerSample, cancellationToken));

        for (var i = 1; i < SampleCount; i++)
        {
            Console.WriteLine($"{i}/{SampleCount} samplings complete");
            DateTime waitUntil = DateTime.Now.AddMinutes(MinutesPerSample);
            while (DateTime.Now < waitUntil)
            {
                var timeLeft = waitUntil - DateTime.Now;
                Console.Write($"\rTime until next sampling: {timeLeft:mm':'ss}");
                await Task.Delay(250, cancellationToken);
            }
            Console.WriteLine();
            allData.AddRange(await SampleData(MinutesPerSample, cancellationToken));
        }
        Console.WriteLine("Sampling complete");

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
        var monitoringDataUrl = $"{monitoringUrl}/monitored-endpoints?history={minutes}";

        // Lots of retries here because we get multiple queues in one go
        var arr = await GetServiceControlData<JArray>(monitoringDataUrl, cancellationToken, 5);

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
        Console.WriteLine($"Getting throughput from {endpointName} using audit data.");

        try
        {
            return await GetThroughputFromAuditsInternal(endpointName, cancellationToken);
        }
        catch (ServiceControlDataException x)
        {
            ConsoleHelper.WriteError($"Warning: Unable to read ServiceControl data from {x.Url} after {x.Attempts} attempts: {x.Message}");
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
        var url = $"{primaryUrl}/endpoints/{endpointName}/messages/?page={page}&per_page={pageSize}&sort=processed_at&direction=desc";

        var arr = await GetServiceControlData<JArray>(url, cancellationToken);

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
        knownEndpoints = await GetKnownEndpoints(cancellationToken);

        if (!knownEndpoints.Any())
        {
            ConsoleHelper.WriteError("Successfully connected to ServiceControl API but no known endpoints could be found. Are you using the correct URL?");
            Environment.Exit(1);
        }

        var configUrl = $"{primaryUrl}/configuration";

        // Tool can't proceed without this data, try 5 times
        var obj = await GetServiceControlData<JObject>(configUrl, cancellationToken, 5);

        var transportTypeToken = obj["transport"]["transport_customization_type"];
        if (transportTypeToken is null)
        {
            ConsoleHelper.WriteError("This version of ServiceControl is not supported. Update to a supported version of ServiceControl.");
            ConsoleHelper.WriteError("See https://docs.particular.net/servicecontrol/upgrades/supported-versions");
            Environment.Exit(1);
        }

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
        var endpointsUrl = $"{primaryUrl}/endpoints";

        // Tool can't proceed without this data, try 5 times
        var arr = await GetServiceControlData<JArray>(endpointsUrl, cancellationToken, 5);

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
            var messagesUrl = $"{primaryUrl}/endpoints/{endpoint.Name}/messages/?per_page=1";

            var recentMessages = await GetServiceControlData<JArray>(messagesUrl, cancellationToken, 2);

            endpoint.AuditedMessages = recentMessages.Any();
        }

        return endpoints;
    }

    async Task<TJsonType> GetServiceControlData<TJsonType>(string url, CancellationToken cancellationToken, int tryCount = 1)
        where TJsonType : JToken
    {
        for (int i = 0; i < tryCount; i++)
        {
            try
            {
                using (var stream = await http.GetStreamAsync(url, cancellationToken))
                using (var reader = new StreamReader(stream))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    return serializer.Deserialize<TJsonType>(jsonReader);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception x)
            {
                if (i + 1 >= tryCount)
                {
                    throw new ServiceControlDataException(url, tryCount, x);
                }
            }
        }

        throw new InvalidOperationException("Retry loop ended without returning or throwing. This should not happen.");
    }

    [Conditional("DEBUG")]
    void Debug(string message)
    {
        Console.WriteLine(message);
    }

    class ServiceControlEndpoint
    {
        public string Name { get; set; }
        public bool HeartbeatsEnabled { get; set; }
        public bool ReceivingHeartbeats { get; set; }
        public bool AuditedMessages { get; set; }
    }
}