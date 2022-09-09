using System;
using System.CommandLine;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Particular.EndpointThroughputCounter.Data;

class AzureServiceBusCommand : BaseCommand
{
    public static Command CreateCommand()
    {
        var command = new Command("azureservicebus", "Measure endpoints and throughput using Azure Service Bus metrics");

        var resourceIdArg = new Option<string>(
            name: "--resourceId",
            description: "The resource id for the Azure Service Bus namespace, which can be found in the Properties page in the Azure Portal.");

        command.AddOption(resourceIdArg);

        var maskNames = SharedOptions.CreateMaskNamesOption();
        command.AddOption(maskNames);

        command.SetHandler(async (resourceId, maskNames) =>
        {
            var runner = new AzureServiceBusCommand(maskNames, resourceId);
            await runner.Run(CancellationToken.None);
        },
        resourceIdArg, maskNames);

        return command;
    }

    readonly string resourceId;

    public AzureServiceBusCommand(string[] maskNames, string resourceId)
    : base(maskNames)
    {
        this.resourceId = resourceId;
    }

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var command = $"az monitor metrics list --resource {resourceId} --dimension EntityName --aggregation Total --start-time {startTime:yyyy-MM-dd}T00:00:00+00:00 --end-time {endTime:yyyy-MM-dd}T00:00:00+00:00 --interval 24h --metrics CompleteMessage";

        var jsonText = AzCommand(command);

        var json = JsonConvert.DeserializeObject<JObject>(jsonText);

        // First value because we only ask for CompleteMessage metrics
        var completeMessageStats = json["value"][0] as JObject;
        var timeseries = completeMessageStats["timeseries"] as JArray;

        var data = timeseries.Select(queueData =>
        {
            var queueName = (queueData["metadatavalues"] as JArray)
                .Where(pair => pair["name"]["value"].Value<string>() == "entityname")
                .Select(pair => pair["value"].Value<string>())
                .FirstOrDefault();

            var maxThroughput = (queueData["data"] as JArray)
                .Select(dayData => dayData["total"].Value<int>())
                .Max();

            return new QueueThroughput { QueueName = queueName, Throughput = maxThroughput };
        })
        .ToArray();

        return new QueueDetails
        {
            StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
            EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
            Queues = data
        };
    }

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "AzureServiceBus",
            ReportMethod = "AzureServiceBus Metrics"
        });
    }

    string AzCommand(string command)
    {
        var tmpPath = Path.GetTempPath();

        var name = Path.GetRandomFileName().Replace(".", "");

        var outFile = Path.Combine(tmpPath, $"{name}-stdout.txt");
        var errFile = Path.Combine(tmpPath, $"{name}-stderr.txt");

        try
        {
            var fullCmd = $"{command} > \"{outFile}\" 2> \"{errFile}\"";
            var psCommandBytes = System.Text.Encoding.Unicode.GetBytes(fullCmd);
            var psCommandBase64 = Convert.ToBase64String(psCommandBytes);

            var p = new Process();
            p.StartInfo.UseShellExecute = true;
            p.StartInfo.FileName = GetPowershellExe();
            p.StartInfo.Arguments = $"-NoProfile -ExecutionPolicy unrestricted -EncodedCommand {psCommandBase64}";
            p.StartInfo.WorkingDirectory = Environment.GetEnvironmentVariable("USERPROFILE");

            p.Start();
            p.WaitForExit();

            var output = File.Exists(outFile) ? File.ReadAllText(outFile) : null;
            var error = File.Exists(errFile) ? File.ReadAllText(errFile) : null;

            if (!string.IsNullOrEmpty(error))
            {
                throw new Exception("Azure CLI command completed with error: " + error.Trim());
            }
            else if (p.ExitCode != 0)
            {
                throw new Exception("Azure CLI command completed with exit code {p.ExitCode} but no output to stderr.");
            }

            return output;
        }
        finally
        {
            if (File.Exists(outFile))
            {
                File.Delete(outFile);
            }
            if (File.Exists(errFile))
            {
                File.Delete(errFile);
            }
        }
    }

    string GetPowershellExe()
    {
        string[] options = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? new[] { "pwsh.exe", "powershell.exe" }
            : new[] { "pwsh" };

        foreach (var option in options)
        {
            if (TestExecutable(option))
            {
                return option;
            }
        }

        throw new Exception("No Powershell executable found");
    }

    bool TestExecutable(string exe)
    {
        var p = new Process();
        p.StartInfo.UseShellExecute = true;
        p.StartInfo.FileName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "where.exe" : "where";
        p.StartInfo.Arguments = exe;
        p.StartInfo.WorkingDirectory = Environment.GetEnvironmentVariable("USERPROFILE");

        p.Start();

        p.WaitForExit();

        return p.ExitCode == 0;
    }
}