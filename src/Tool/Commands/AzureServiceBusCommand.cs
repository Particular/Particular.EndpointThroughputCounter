﻿using System;
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

        command.SetHandler(async context =>
        {
            var shared = SharedOptions.Parse(context);
            var resourceId = context.ParseResult.GetValueForOption(resourceIdArg);
            var cancellationToken = context.GetCancellationToken();

            var runner = new AzureServiceBusCommand(shared, resourceId);
            await runner.Run(cancellationToken);
        });

        return command;
    }

    readonly string resourceId;

    public AzureServiceBusCommand(SharedOptions shared, string resourceId)
    : base(shared)
    {
        this.resourceId = resourceId;
    }

#pragma warning disable CS1998 // Haven't been able to get AzCommand to work async yet, currently synchronous

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        var endTime = DateTime.UtcNow.Date.AddDays(1);
        var startTime = endTime.AddDays(-30);

        var queueNames = await GetQueueNames(cancellationToken);

        Console.WriteLine($"Found {queueNames.Length} queues");

        var data = queueNames
            .Select((queueName, index) =>
            {
                Console.WriteLine($"Gathering metrics for queue {index + 1}/{queueNames.Length}: {queueName}");
                var command = $"az monitor metrics list --resource {resourceId} --aggregation Total --start-time {startTime:yyyy-MM-dd}T00:00:00+00:00 --end-time {endTime:yyyy-MM-dd}T00:00:00+00:00 --interval 24h --metrics CompleteMessage --filter \"EntityName eq '{queueName}'\"";

                var jsonText = AzCommand(command);

                var json = JsonConvert.DeserializeObject<JObject>(jsonText);

                // First value because we only ask for CompleteMessage metrics
                var completeMessageStats = json["value"][0] as JObject;
                var timeseries = completeMessageStats["timeseries"] as JArray;

                return timeseries.Select(queueData =>
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
                .SingleOrDefault();
            })
            .Where(d => d != null)
            .ToArray();

        return new QueueDetails
        {
            StartTime = new DateTimeOffset(startTime, TimeSpan.Zero),
            EndTime = new DateTimeOffset(endTime, TimeSpan.Zero),
            Queues = data,
            TimeOfObservation = TimeSpan.FromDays(1)
        };
    }

    async Task<string[]> GetQueueNames(CancellationToken cancellationToken)
    {
        var parts = resourceId.Split('/');
        if (parts.Length != 9 || parts[0] != string.Empty || parts[1] != "subscriptions" || parts[3] != "resourceGroups" || parts[5] != "providers" || parts[6] != "Microsoft.ServiceBus" || parts[7] != "namespaces")
        {
            throw new Exception("The provided --resourceId value does not look like an Azure Service Bus resourceId. A correct value should take the form '/subscriptions/{GUID}/resourceGroups/{NAME}/providers/Microsoft.ServiceBus/namespaces/{NAME}'.");
        }

        var rg = parts[4];
        var name = parts[8];

        var command = $"az servicebus queue list --namespace-name {name} --resource-group {rg}";

        var jsonText = AzCommand(command);

        var json = JsonConvert.DeserializeObject<JArray>(jsonText);

        return json.Select(token => token["name"].Value<string>())
            .OrderBy(name => name)
            .ToArray();
    }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

    protected override Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new EnvironmentDetails
        {
            MessageTransport = "AzureServiceBus",
            ReportMethod = "AzureServiceBus Metrics",
            SkipEndpointListCheck = true
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
        var start = p.StartInfo;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            start.UseShellExecute = true;
            start.FileName = "where.exe";
            start.Arguments = exe;
            start.WorkingDirectory = Environment.GetEnvironmentVariable("USERPROFILE");
        }
        else
        {
            start.FileName = "/bin/bash";
            start.Arguments = $"-c \"which {exe}\"";
            start.UseShellExecute = false;
            start.CreateNoWindow = true;
        }

        p.Start();
        p.WaitForExit();

        return p.ExitCode == 0;
    }
}