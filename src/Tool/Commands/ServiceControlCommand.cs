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
        var outputPath = SharedOptions.CreateOutputPathOption();
        command.AddOption(maskNames);
        command.AddOption(outputPath);

        command.SetHandler(async (scUrl, monUrl, maskNames, outputPath) =>
        {
            var runner = new ServiceControlCommand(outputPath, maskNames, scUrl, monUrl);
            await runner.Run(CancellationToken.None);
        },
        scUrlArg, monitoringUrlArg, maskNames, outputPath);

        return command;
    }

    readonly AuthenticatingHttpClient http;
    readonly JsonSerializer serializer;
    readonly string primaryUrl;
    readonly string monitoringUrl;

#if DEBUG
    // So that a run can be done in 3 minutes in debug mode
    const int samples = 3;
    const int minutesPerSample = 1;
#else
    const int samples = 24;
    const int minutesPerSample = 60;
#endif

    public ServiceControlCommand(string outputPath, string[] maskNames, string primaryUrl, string monitoringUrl)
        : base(outputPath, maskNames)
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

        var knownEndpointsUrl = $"{primaryUrl}/endpoints/known";

        using (var stream = await http.GetStreamAsync(knownEndpointsUrl, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var arr = serializer.Deserialize<JArray>(jsonReader);

            var knownEndpoints = arr.Select(token => token["endpoint_details"]["name"].Value<string>())
                .ToArray();

            foreach (var knownEndpoint in knownEndpoints)
            {
                if (!recordedEndpoints.Contains(knownEndpoint))
                {
                    queues.Add(new QueueThroughput
                    {
                        QueueName = knownEndpoint,
                        Throughput = -1
                    });
                }
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

        using (var stream = await http.GetStreamAsync(monitoringDataUrl, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var arr = serializer.Deserialize<JArray>(jsonReader);

            return arr.Select(token =>
            {
                var name = token["name"].Value<string>();
                var throughputAvgPerSec = token["metrics"]["throughput"]["average"].Value<double>();
                var throughputTotal = throughputAvgPerSec * minutes * 60;

                return new QueueThroughput
                {
                    QueueName = name,
                    Throughput = (int)throughputTotal
                };
            })
            .ToArray();
        }
    }

    protected override async Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        var configUrl = $"{primaryUrl}/configuration";

        using (var stream = await http.GetStreamAsync(configUrl, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var obj = serializer.Deserialize<JObject>(jsonReader);

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
                ReportMethod = "ServiceControl API"
            };
        }
    }
}