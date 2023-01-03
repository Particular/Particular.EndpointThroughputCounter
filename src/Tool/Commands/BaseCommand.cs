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
    readonly SharedOptions shared;
    readonly string reportName = "throughput-report";
    readonly bool isDevelopment;

    public BaseCommand(SharedOptions shared)
    {
        this.shared = shared;

        var envVars = Environment.GetEnvironmentVariables().Keys.OfType<string>().OrderBy(x => x).ToArray();

        if (!bool.TryParse(Environment.GetEnvironmentVariable("IS_DEVELOPMENT"), out isDevelopment))
        {
            isDevelopment = false;
        }
    }

    string CreateReportOutputPath(string customerName)
    {
        var customerFileName = Regex.Replace(customerName, @"[^\w\d]+", "-").Trim('-').ToLower();
        var outputPath = Path.Join(Environment.CurrentDirectory,
            $"{customerFileName}-{reportName}-{DateTime.Now:yyyyMMdd-HHmmss}.json");

        return outputPath;
    }

    void ValidateOutputPath(string outputPath)
    {
        if (File.Exists(outputPath) && !isDevelopment)
        {
            throw new HaltException(HaltReason.OutputFile, $"ERROR: File already exists at {outputPath}, running would overwrite");
        }

        try
        {
            using (new StreamWriter(outputPath, false))
            {
            }
        }
        catch (Exception x)
        {
            throw new HaltException(HaltReason.OutputFile, $"ERROR: Unable to write to output file at {outputPath}: {x.Message}");
        }
    }

    public async Task Run(CancellationToken cancellationToken = default)
    {
        try
        {
            await RunInternal(cancellationToken);
        }
        catch (HaltException halt)
        {
            Console.WriteLine();
            Console.WriteLine();
            ConsoleHelper.WriteError(halt.Message);
            Environment.ExitCode = halt.ExitCode;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine();
            Console.WriteLine();
            ConsoleHelper.WriteError("Exiting because cancellation was requested.");
            Environment.ExitCode = -1;
        }
        catch (Exception x)
        {
            ConsoleHelper.WriteError(w =>
            {
                w.WriteLine(x);
                w.WriteLine();
                w.WriteLine("Unable to run tool, please contact Particular Software support.");
            });

            Environment.ExitCode = -2;
        }
    }

    async Task RunInternal(CancellationToken cancellationToken)
    {
        Console.WriteLine();
        if (!string.IsNullOrEmpty(shared.CustomerName))
        {
            Console.WriteLine($"Customer name is '{shared.CustomerName}'.");
        }
        else
        {
            while (string.IsNullOrEmpty(shared.CustomerName))
            {
                cancellationToken.ThrowIfCancellationRequested();
                Console.Write("Enter customer name: ");
                shared.CustomerName = Console.ReadLine();
            }
        }

#if !DEBUG
        if (string.Equals(shared.CustomerName, "Particular Software", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new HaltException(12, "Customer name 'Particular Software' is not allowed.");
        }
#endif

        var outputPath = CreateReportOutputPath(shared.CustomerName);

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

            if (!shared.RunUnattended)
            {
                if (!Confirm("Do you wish to proceed?"))
                {
                    throw new HaltException(HaltReason.UserCancellation, "Exiting at user's request");
                }
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
            CustomerName = shared.CustomerName,
            MessageTransport = metadata.MessageTransport,
            ReportMethod = metadata.ReportMethod,
            ToolVersion = Versioning.NuGetVersion,
            Prefix = metadata.Prefix,
            StartTime = data.StartTime,
            EndTime = data.EndTime,
            ReportDuration = data.TimeOfObservation ?? data.EndTime - data.StartTime,
            Queues = data.Queues,
            TotalThroughput = data.Queues.Sum(q => q.Throughput ?? 0),
            TotalQueues = data.Queues.Length,
            IgnoredQueues = metadata.IgnoredQueues?.Select(q => MaskName(q)).ToArray()
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
        foreach (string mask in shared.MaskNames)
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
