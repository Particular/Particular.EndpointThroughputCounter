using System.CommandLine;
using LicenseTool.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

    readonly HttpClient http;
    readonly JsonSerializer serializer;
    readonly string primaryUrl;
    readonly string monitoringUrl;

    public ServiceControlCommand(string outputPath, string[] maskNames, string primaryUrl, string monitoringUrl)
        : base(outputPath, maskNames)
    {
        this.primaryUrl = primaryUrl.TrimEnd('/');
        this.monitoringUrl = monitoringUrl.TrimEnd('/');

        http = new HttpClient();
        serializer = new JsonSerializer();
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        int minutes = 15;
        var monitoringDataUrl = $"{monitoringUrl}/monitored-endpoints?history={minutes}";

        List<QueueThroughput> queues;

        using (var stream = await http.GetStreamAsync(monitoringDataUrl, cancellationToken))
        using (var reader = new StreamReader(stream))
        using (var jsonReader = new JsonTextReader(reader))
        {
            var arr = serializer.Deserialize<JArray>(jsonReader);

            queues = arr.Select(token =>
            {
                var name = token["name"].Value<string>();
                var throughputAvgPerSec = token["metrics"]["throughput"]["average"].Value<double>();
                var throughputTotal = throughputAvgPerSec * 60 * minutes;

                return new QueueThroughput
                {
                    QueueName = name,
                    Throughput = (int)throughputTotal
                };
            })
            .ToList();
        }

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
            StartTime = now - TimeSpan.FromMinutes(minutes),
            EndTime = now,
            Queues = queues.OrderBy(q => q.QueueName).ToArray()
        };
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