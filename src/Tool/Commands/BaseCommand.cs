using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Particular.EndpointThroughputCounter.Data;

abstract class BaseCommand
{
    readonly string[] maskNames;
    readonly string reportName = "throughput-report";
    readonly bool isDevelopment;

    public BaseCommand(string[] maskNames)
    {
        this.maskNames = maskNames;

        var envVars = Environment.GetEnvironmentVariables().Keys.OfType<string>().OrderBy(x => x).ToArray();

        if (!bool.TryParse(Environment.GetEnvironmentVariable("IS_DEVELOPMENT"), out isDevelopment))
        {
            isDevelopment = false;
        }
    }

    string GenerateReportTimeStamp(DateTime dateTime)
    {
        return $"{dateTime.Year}{dateTime.Month}-{dateTime.Day}-{dateTime.Hour}-{dateTime.Minute}-{dateTime.Second}";
    }

    string CreateReportOutputPath(string customerName)
    {
        var customerFileName = Regex.Replace(customerName, @"[^\w\d]+", "-").Trim('-').ToLower();
        var outputPath = Path.Join(Environment.CurrentDirectory,
            $"{customerFileName}-{reportName}-{GenerateReportTimeStamp(DateTime.Now)}");

        return outputPath;
    }

    void ValidateOutputPath(string outputPath)
    {
        if (File.Exists(outputPath) && !isDevelopment)
        {
            Console.Error.WriteLine($"ERROR: File already exists at {outputPath}, running would overwrite");
            Environment.Exit(1);
        }

        try
        {
            using (new StreamWriter(outputPath, false))
            {
            }
        }
        catch (Exception x)
        {
            Console.Error.WriteLine($"ERROR: Unable to write to output file at {outputPath}: {x.Message}");
            Environment.Exit(1);
        }
    }

    public async Task Run(CancellationToken cancellationToken = default)
    {
        Console.WriteLine();
        Console.Write("Enter customer name: ");
        string customerName = Console.ReadLine();

        var outputPath = CreateReportOutputPath(customerName);

        ValidateOutputPath(outputPath);

        Console.WriteLine();

        Console.WriteLine("Collecting environment info...");
        var metadata = await GetEnvironment(cancellationToken);

        if (!metadata.SkipEndpointListCheck)
        {
            var mappedQueueNames = metadata.QueueNames
                .Select(name => new { Name = name, Masked = MaskName(name) })
                .ToArray();

            Console.WriteLine();
            Console.WriteLine("Writing endpoint/queue names discovered:");
            Console.WriteLine();

            const string leftLabel = "Queue/Endpoint Name";
            const string rightLabel = "Will be reported as";
            var leftWidth = Math.Max(leftLabel.Length, metadata.QueueNames.Select(name => name.Length).Max());
            var rightWidth = Math.Max(rightLabel.Length, mappedQueueNames.Select(set => set.Masked.Length).Max());

            var lineFormat = $" {{0,-{leftWidth}}} | {{1,-{rightWidth}}}";

            Console.WriteLine(lineFormat, leftLabel, rightLabel);
            Console.WriteLine(lineFormat, new string('-', leftWidth), new string('-', rightWidth));
            foreach (var set in mappedQueueNames)
            {
                Console.WriteLine(lineFormat, set.Name, set.Masked);
            }
            Console.WriteLine();

            Console.WriteLine("The right column shows how queue names will be reported. If queue names contain sensitive");
            Console.WriteLine("or proprietary information, the names can be masked using the --queueNameMasks parameter.");
            Console.WriteLine();
            if (!Confirm("Do you wish to proceed?"))
            {
                Console.WriteLine("Exiting...");
                Environment.Exit(1);
            }
        }
        Console.WriteLine();

        var data = await GetData(cancellationToken);

        foreach (var q in data.Queues)
        {
            q.QueueName = MaskName(q.QueueName);
        }

        var reportData = new Report
        {
            CustomerName = customerName,
            MessageTransport = metadata.MessageTransport,
            ReportMethod = metadata.ReportMethod,
            ToolVersion = Versioning.NuGetVersion,
            StartTime = data.StartTime,
            EndTime = data.EndTime,
            ReportDuration = data.TimeOfObservation ?? data.EndTime - data.StartTime,
            Queues = data.Queues,
            TotalThroughput = data.Queues.Sum(q => q.Throughput),
            TotalQueues = data.Queues.Length
        };

        var report = new SignedReport
        {
            ReportData = reportData,
            Signature = Signature.SignReport(reportData)
        };

        var ser = new JsonSerializer();

        Console.WriteLine();
        Console.WriteLine($"Writing report to {outputPath}");
        using (var writer = new StreamWriter(outputPath, false))
        using (var jsonWriter = new JsonTextWriter(writer))
        {
            jsonWriter.Formatting = Formatting.Indented;
            ser.Serialize(jsonWriter, report, typeof(SignedReport));
        }
        Console.WriteLine("EndpointThroughputTool complete.");
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0010:Add missing cases", Justification = "Don't need every key")]
    bool Confirm(string prompt)
    {
        Console.Write(prompt);
        Console.Write(" (Y/N): ");
        while (true)
        {
            var key = Console.ReadKey(true);
            switch (key.Key)
            {
                case ConsoleKey.Y:
                    Console.WriteLine("Yes");
                    return true;
                case ConsoleKey.N:
                    Console.WriteLine("No");
                    return false;
                default:
                    continue;
            }
        }
    }

    string MaskName(string queueName)
    {
        foreach (string mask in maskNames)
        {
            queueName = queueName.Replace(mask, "***", StringComparison.OrdinalIgnoreCase);
        }

        return queueName;
    }

    protected abstract Task<QueueDetails> GetData(CancellationToken cancellationToken = default);

    protected abstract Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default);

#if DEBUG
    protected TimeSpan PollingRunTime = TimeSpan.FromMinutes(1);
#else
    protected TimeSpan PollingRunTime = TimeSpan.FromDays(1);
#endif

}