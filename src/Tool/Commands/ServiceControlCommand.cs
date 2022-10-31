using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Particular.EndpointThroughputCounter.Data;

class ServiceControlCommand : BaseCommand
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

        var maskNames = SharedOptions.CreateMaskNamesOption();
        command.AddOption(maskNames);

        command.SetHandler(async (scUrl, monUrl, maskNames) =>
        {
            var runner = new ServiceControlCommand(maskNames, scUrl, monUrl);
            await runner.Run(CancellationToken.None);
        },
        scUrlArg, monitoringUrlArg, maskNames);

        return command;
    }

    readonly AuthenticatingHttpClient http;
    readonly JsonSerializer serializer;
    readonly string primaryUrl;
    readonly string monitoringUrl;
    ServiceControlEndpoint[] knownEndpoints;

#if DEBUG
    // So that a run can be done in 3 minutes in debug mode
    const int samples = 3;
    const int minutesPerSample = 1;
    const int auditSamplingPageSize = 5;
#else
    const int samples = 24;
    const int minutesPerSample = 60;
    const int auditSamplingPageSize = 500;
#endif

    public ServiceControlCommand(string[] maskNames, string primaryUrl, string monitoringUrl)
        : base(maskNames)
    {
        this.primaryUrl = primaryUrl.TrimEnd('/');
        this.monitoringUrl = monitoringUrl.TrimEnd('/');

        http = new AuthenticatingHttpClient();
        serializer = new JsonSerializer();
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var allData = new List<QueueThroughput>();

        var start = DateTimeOffset.Now.AddMinutes(-minutesPerSample);

        Console.WriteLine($"The tool will sample ServiceControl data {samples} times at {minutesPerSample}-minute intervals.");

        Console.WriteLine("Performing initial data sampling...");
        allData.AddRange(await SampleData(minutesPerSample, cancellationToken));

        for (var i = 1; i < samples; i++)
        {
            Console.WriteLine($"{i}/{samples} samplings complete");
            DateTime waitUntil = DateTime.Now.AddMinutes(minutesPerSample);
            while (DateTime.Now < waitUntil)
            {
                var timeLeft = waitUntil - DateTime.Now;
                Console.Write($"\rTime until next sampling: {timeLeft:mm':'ss}");
                await Task.Delay(250, cancellationToken);
            }
            Console.WriteLine();
            allData.AddRange(await SampleData(minutesPerSample, cancellationToken));
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

        var arr = await GetServiceControlData<JArray>(monitoringDataUrl, cancellationToken);

        var results = arr.Select(token =>
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

        var monitoredNames = results.Select(ep => ep.QueueName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var endpoint in knownEndpoints.Where(ep => !monitoredNames.Contains(ep.Name) && ep.AuditedMessages))
        {
            var fromAuditing = await GetThroughputFromAudits(endpoint.Name, cancellationToken);
            if (fromAuditing is not null)
            {
                results.Add(fromAuditing);
            }
        }

        return results.ToArray();
    }

    async Task<QueueThroughput> GetThroughputFromAudits(string endpointName, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Getting throughput from {endpointName} using audit data.");

        var sampleStart = DateTime.UtcNow.AddMinutes(-minutesPerSample);

        async Task<AuditBatch> GetPage(int page)
        {
            Console.WriteLine($"  * Getting page {page}");
            return await GetAuditBatch(endpointName, page, auditSamplingPageSize, cancellationToken);
        }

        var firstPage = await GetPage(1);

        if (firstPage.ContainsTime(sampleStart))
        {
            return new QueueThroughput { QueueName = endpointName, Throughput = firstPage.CountGreaterThan(sampleStart) };
        }

        var estimatedMessagesThisSample = TimeSpan.FromMinutes(minutesPerSample).TotalSeconds / firstPage.EstimatedSecPerMsg;
        // Go 120% so that if the real math estimate is almost exactly right, the page is ensured to be in the first range for the binary search.
        // Also saves us from double-to-int conversion slicing off the estimate just before where the answer lies.
        var estimatedPages = 1.2 * estimatedMessagesThisSample / auditSamplingPageSize;
        Console.WriteLine($"  * Estimating {estimatedPages:0.0} pages");

        var minPage = 1;
        var maxPage = -1;

        // Start at the best guess for max page, increasing by same amount until we find a minDate that is
        // earlier than an hour ago. Setting maxPage to its real value stops the loop.
        for (var factor = 1; maxPage == -1; factor++)
        {
            var attemptPageNum = (int)(factor * estimatedPages);
            var page = await GetPage(attemptPageNum);

            if (page.ContainsTime(sampleStart))
            {
                return new QueueThroughput
                {
                    QueueName = endpointName,
                    Throughput = (auditSamplingPageSize * (attemptPageNum - 1)) + page.CountGreaterThan(sampleStart)
                };
            }

            if (page.Count == 0 || page.ProcessedAtMin < sampleStart)
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

        Console.WriteLine($"  * Starting binary search with min {minPage}, max {maxPage}");
        // Do a binary search to find the page that represents where 1 hour ago was
        while (minPage != maxPage)
        {
            var middlePageNum = (minPage + maxPage) / 2;
            var pageData = await GetPage(middlePageNum);

            // If we've backtracked to a page we've hit before, or the page actually contains the time we seek, we're done
            if (middlePageNum == minPage || middlePageNum == maxPage || pageData.ContainsTime(sampleStart))
            {
                Console.WriteLine($"  * Found => {(auditSamplingPageSize * (middlePageNum - 1)) + pageData.CountGreaterThan(sampleStart)} messages");
                return new QueueThroughput
                {
                    QueueName = endpointName,
                    Throughput = (auditSamplingPageSize * (middlePageNum - 1)) + pageData.CountGreaterThan(sampleStart)
                };
            }

            if (pageData.Count == 0 || pageData.ProcessedAtMax < sampleStart)
            {
                // Went too far, cut out the top half
                maxPage = middlePageNum;
            }
            else if (pageData.ProcessedAtMin > sampleStart)
            {
                // Not far enough, cut out the bottom half
                minPage = middlePageNum;
            }
        }

        // Likely we don't get here, but for completeness
        var finalPage = await GetPage(minPage);
        Console.WriteLine($"  * Catch-All => {(auditSamplingPageSize * (minPage - 1)) + finalPage.CountGreaterThan(sampleStart)} messages");
        return new QueueThroughput
        {
            QueueName = endpointName,
            Throughput = (auditSamplingPageSize * (minPage - 1)) + finalPage.CountGreaterThan(sampleStart)
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
            Count = timestamps.Length;

            if (Count > 0)
            {
                ProcessedAtMin = timestamps.Min();
                ProcessedAtMax = timestamps.Max();
                EstimatedSecPerMsg = (ProcessedAtMax - ProcessedAtMin).TotalSeconds / Count;
            }
            else
            {
                ProcessedAtMin = default;
                ProcessedAtMax = default;
                EstimatedSecPerMsg = 0;
            }
        }

        DateTime[] timestamps;

        public int Count { get; }
        public DateTime ProcessedAtMin { get; }
        public DateTime ProcessedAtMax { get; }

        public double EstimatedSecPerMsg { get; }

        public bool ContainsTime(DateTime targetTime)
        {
            return ProcessedAtMin <= targetTime && targetTime <= ProcessedAtMax;
        }

        public int CountGreaterThan(DateTime cutoff)
        {
            return timestamps.Count(dt => dt >= cutoff);
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        knownEndpoints = await GetKnownEndpoints(cancellationToken);

        if (!knownEndpoints.Any())
        {
            Console.Error.WriteLine("Successfully connected to ServiceControl API but no known endpoints could be found. Are you using the correct URL?");
            Environment.Exit(1);
        }

        var configUrl = $"{primaryUrl}/configuration";

        var obj = await GetServiceControlData<JObject>(configUrl, cancellationToken);

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

        var arr = await GetServiceControlData<JArray>(endpointsUrl, cancellationToken);

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
            var messagesUrl = $"{primaryUrl}/endpoints/{endpoint.Name}/messages/?per_page=5";

            var recentMessages = await GetServiceControlData<JArray>(messagesUrl, cancellationToken);

            endpoint.AuditedMessages = recentMessages.Any();
        }

        return endpoints;
    }

    async Task<TJsonType> GetServiceControlData<TJsonType>(string url, CancellationToken cancellationToken)
        where TJsonType : JToken
    {
        using (var stream = await http.GetStreamAsync(url, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            return serializer.Deserialize<TJsonType>(jsonReader);
        }
    }

    class ServiceControlEndpoint
    {
        public string Name { get; set; }
        public bool HeartbeatsEnabled { get; set; }
        public bool ReceivingHeartbeats { get; set; }
        public bool AuditedMessages { get; set; }
    }
}