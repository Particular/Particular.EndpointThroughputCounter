﻿using System.Text.Json;
using System.Text.RegularExpressions;
using Particular.EndpointThroughputCounter.Infra;
using Particular.EndpointThroughputCounter.ServiceControl;
using Particular.LicensingComponent.Report;

abstract class BaseCommand
{
    readonly SharedOptions shared;
    readonly string reportName = "throughput-report";
    readonly bool isDevelopment;

    public BaseCommand(SharedOptions shared)
    {
        RunInfo.Add("Command", GetType().Name);
        this.shared = shared;
#if DEBUG
        PollingRunTime = TimeSpan.FromMinutes(1);
#else
        PollingRunTime = TimeSpan.FromHours(shared.RuntimeInHours);
#endif
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
            File.Delete(outputPath);
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
            Out.WriteLine();
            Out.WriteLine();
            Out.WriteError(halt.Message);
            if (halt.InnerException != null)
            {
                Out.WriteLine();
                Out.WriteLine("Original exception message: " + halt.InnerException.Message);
            }
            Environment.ExitCode = halt.ExitCode;
        }
        catch (ServiceControlDataException scX)
        {
            Out.WriteLine();
            Out.WriteLine();
            Out.WriteError("There was a problem getting data from ServiceControl. Are all of the ServiceControl instances running correctly?");
            Out.WriteLine();
            Out.WriteLine("Original exception: " + scX.ToString());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Out.WriteLine();
            Out.WriteLine();
            Out.WriteError("Exiting because cancellation was requested.");
            Environment.ExitCode = (int)HaltReason.UserCancellation;
        }
        catch (Exception x)
        {
            Out.WriteError(w =>
            {
                w.WriteLine(x);
                w.WriteLine();
                w.WriteLine("Unable to run tool, please contact Particular Software support.");
            });

            Exceptions.ReportError(x);

            Environment.ExitCode = (int)HaltReason.RuntimeError;
        }
    }

    async Task RunInternal(CancellationToken cancellationToken)
    {
        if (!await Versioning.EvaluateVersion(shared.SkipVersionCheck, cancellationToken))
        {
            Environment.ExitCode = -1;
            return;
        }

        Out.WriteLine();
        if (!string.IsNullOrEmpty(shared.CustomerName))
        {
            Out.WriteLine($"Customer name is '{shared.CustomerName}'.");
        }
        else
        {
            while (string.IsNullOrEmpty(shared.CustomerName))
            {
                cancellationToken.ThrowIfCancellationRequested();
                Out.Write("Enter customer name: ");
                shared.CustomerName = Out.ReadLine();
            }
        }

#if !DEBUG
        if (string.Equals(shared.CustomerName, "Particular Software", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new HaltException(HaltReason.InvalidConfig, "Customer name 'Particular Software' is not allowed.");
        }
#endif

        var outputPath = CreateReportOutputPath(shared.CustomerName);

        ValidateOutputPath(outputPath);

        Out.WriteLine();

        await Initialize(cancellationToken);

        Out.WriteLine("Collecting environment info...");
        var metadata = await GetEnvironment(cancellationToken);

        var queueNoun = metadata.QueuesAreEndpoints ? "endpoint" : "queue";
        var queueNounUpper = metadata.QueuesAreEndpoints ? "Endpoint" : "Queue";

        if (!metadata.SkipEndpointListCheck)
        {
            if (!metadata.QueueNames.Any())
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"No {queueNoun}s could be discovered. Please check to make sure your configuration is correct.");
            }

            var mappedQueueNames = metadata.QueueNames
                .Select(name => new { Name = name, Masked = shared.Mask(name) })
                .ToArray();

            Out.WriteLine();
            Out.WriteLine($"Writing {queueNoun} names discovered:");
            Out.WriteLine();

            string leftLabel = $"{queueNounUpper} Name";
            const string rightLabel = "Will be reported as";
            var leftWidth = Math.Max(leftLabel.Length, metadata.QueueNames.Select(name => name.Length).Max());
            var rightWidth = Math.Max(rightLabel.Length, mappedQueueNames.Select(set => set.Masked.Length).Max());

            var lineFormat = $" {{0,-{leftWidth}}} | {{1,-{rightWidth}}}";

            Out.WriteLine(lineFormat, leftLabel, rightLabel);
            Out.WriteLine(lineFormat, new string('-', leftWidth), new string('-', rightWidth));
            foreach (var set in mappedQueueNames)
            {
                Out.WriteLine(lineFormat, set.Name, set.Masked);
            }
            Out.WriteLine();

            Out.WriteLine($"The right column shows how {queueNoun} names will be reported. If {queueNoun} names contain sensitive");
            Out.WriteLine("or proprietary information, the names can be masked using the --queueNameMasks parameter.");
            Out.WriteLine();

            if (!metadata.QueuesAreEndpoints)
            {
                Out.WriteLine("Not all queues represent logical endpoints. So, although data from all queues will be included");
                Out.WriteLine("in the report, not all the queues will automatically be included the licensed endpoint count.");
            }

            if (!shared.RunUnattended)
            {
                if (!Out.Confirm("Do you wish to proceed?"))
                {
                    throw new HaltException(HaltReason.UserCancellation, "Exiting at user's request");
                }
            }
        }
        Out.WriteLine();

        Report reportData;

        if (!shared.SkipThroughputCollection)
        {
            var data = await GetData(cancellationToken);

            foreach (var q in data.Queues)
            {
                q.QueueName = shared.Mask(q.QueueName);
                if (q.Throughput.HasValue)
                {
                    q.Throughput = Math.Abs(q.Throughput.Value);
                }
            }

            reportData = new Report
            {
                CustomerName = shared.CustomerName,
                MessageTransport = metadata.MessageTransport,
                ReportMethod = shared.Mask(metadata.ReportMethod),
                ToolType = "Throughput Tool",
                ToolVersion = Versioning.NuGetVersion,
                Prefix = metadata.Prefix,
                ScopeType = data.ScopeType,
                StartTime = data.StartTime,
                EndTime = data.EndTime,
                ReportDuration = data.TimeOfObservation ?? data.EndTime - data.StartTime,
                Queues = data.Queues,
                TotalThroughput = data.Queues.Sum(q => q.Throughput ?? 0),
                TotalQueues = data.Queues.Length,
                IgnoredQueues = metadata.IgnoredQueues?.Select(q => shared.Mask(q)).ToArray()
            };
        }
        else
        {
            var mappedQueueNames = metadata.QueueNames
                .Select(name => new { Name = name, Masked = shared.Mask(name) })
                .ToArray();
            reportData = new Report
            {
                CustomerName = shared.CustomerName,
                MessageTransport = metadata.MessageTransport,
                ReportMethod = shared.Mask(metadata.ReportMethod),
                ToolType = "Throughput Tool",
                ToolVersion = Versioning.NuGetVersion,
                Prefix = metadata.Prefix,
                StartTime = new DateTimeOffset(DateTime.UtcNow.Date, TimeSpan.Zero),
                EndTime = new DateTimeOffset(DateTime.UtcNow.Date.AddDays(1), TimeSpan.Zero),
                ReportDuration = TimeSpan.FromDays(1),
                Queues = mappedQueueNames.Select(map => new QueueThroughput { QueueName = map.Masked, Throughput = 0 }).ToArray(),
                TotalThroughput = 0,
                TotalQueues = mappedQueueNames.Length,
                IgnoredQueues = metadata.IgnoredQueues?.Select(q => shared.Mask(q)).ToArray()
            };
        }

        var report = new SignedReport
        {
            ReportData = reportData,
            Signature = Signature.SignReport(reportData)
        };

        Out.WriteLine();
        Out.WriteLine($"Writing report to {outputPath}");
        File.WriteAllBytes(outputPath, JsonSerializer.SerializeToUtf8Bytes(report, SerializationOptions.IndentedWithNoEscaping));

        Out.WriteLine("EndpointThroughputTool complete.");
    }

    protected virtual Task Initialize(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    protected abstract Task<QueueDetails> GetData(CancellationToken cancellationToken = default);

    protected abstract Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default);

    protected TimeSpan PollingRunTime;
}