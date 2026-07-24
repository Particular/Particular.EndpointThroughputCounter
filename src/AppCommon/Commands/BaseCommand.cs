using System.Text.Json;
using System.Text.RegularExpressions;
using Particular.EndpointThroughputCounter.Infra;
using Particular.EndpointThroughputCounter.ServiceControl;
using Particular.LicensingComponent.Report;
using Particular.LicensingComponent.Report.Utility;

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
                var original = halt.GetBaseException();
                if (original != halt && !halt.Message.Contains(original.Message))
                {
                    Out.WriteLine();
                    Out.WriteLine("Original exception message: " + original.Message);
                }

                Exceptions.ReportError(halt);
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

            void OutputMaskMapping(string unmaskedLabel, string contentLabel, string[] unmaskedContents)
            {
                Out.WriteLine();
                Out.WriteLine($"Writing {contentLabel} discovered:");
                Out.WriteLine();

                (string Unmasked, string Masked)[] contents = [.. unmaskedContents.Select(x => (x, shared.Mask(x)))];
                const string maskedLabel = "Will be reported as";
                var unmaskedWidth = Math.Max(unmaskedLabel.Length, contents.Max(content => content.Unmasked.Length));
                var maskedWidth = Math.Max(maskedLabel.Length, contents.Max(content => content.Masked.Length));

                var lineFormat = $" {{0,-{unmaskedWidth}}} | {{1,-{maskedWidth}}}";

                Out.WriteLine(lineFormat, unmaskedLabel, maskedLabel);
                Out.WriteLine(lineFormat, new string('-', unmaskedWidth), new string('-', maskedWidth));
                foreach (var (unmasked, masked) in contents)
                {
                    Out.WriteLine(lineFormat, unmasked, masked);
                }

                Out.WriteLine();
                Out.WriteLine($"The right column shows how {contentLabel} will be reported. If {contentLabel} contain sensitive");
                Out.WriteLine("or proprietary information, the names can be masked using the --queueNameMasks parameter.");
            }

            var mappedQueueNames = metadata.QueueNames
                .Select(queue => new { Name = queue.QueueName, MaskedName = shared.Mask(queue.QueueName) })
                .ToArray();

            OutputMaskMapping($"{queueNounUpper} Name", $"{queueNoun} names", [.. metadata.QueueNames.Select(queue => queue.QueueName)]);

            var scopes = metadata.QueueNames
                .Where(queue => !string.IsNullOrEmpty(queue.Scope))
                .Select(queue => queue.Scope)
                .Distinct()
                .ToArray();

            if (scopes.Any())
            {
                OutputMaskMapping("Scope", "scopes", scopes);
            }

            if (!metadata.QueuesAreEndpoints)
            {
                Out.WriteLine();
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
                ReportDuration = data.TimeOfObservation ?? (data.EndTime - data.StartTime),
                Queues = [..data.Queues.Select(q => q with
                {
                    NameHash = OneWayHasher.CalculateOneWayHash(q.QueueName),
                    QueueName = shared.Mask(q.QueueName),
                    Scope = string.IsNullOrEmpty(q.Scope) ? q.Scope : shared.Mask(q.Scope),
                    ScopeHash = string.IsNullOrEmpty(q.Scope) ? "" : OneWayHasher.CalculateOneWayHash(q.Scope),
                    Throughput = q.Throughput.HasValue ? Math.Abs(q.Throughput.Value) : null,
                })],
                TotalThroughput = data.Queues.Sum(q => q.Throughput ?? 0),
                TotalQueues = data.Queues.Length,
                IgnoredQueues = metadata.IgnoredQueues?.Select(q => shared.Mask(q)).ToArray()
            };
        }
        else
        {
            reportData = new Report
            {
                CustomerName = shared.CustomerName,
                MessageTransport = metadata.MessageTransport,
                ReportMethod = shared.Mask(metadata.ReportMethod),
                ToolType = "Throughput Tool",
                ToolVersion = Versioning.NuGetVersion,
                Prefix = metadata.Prefix,
                ScopeType = metadata.ScopeType,
                StartTime = new DateTimeOffset(DateTime.UtcNow.Date, TimeSpan.Zero),
                EndTime = new DateTimeOffset(DateTime.UtcNow.Date.AddDays(1), TimeSpan.Zero),
                ReportDuration = TimeSpan.FromDays(1),
                Queues = [.. metadata.QueueNames.Select(q => new QueueThroughput
                {
                    QueueName = shared.Mask(q.QueueName),
                    NameHash = OneWayHasher.CalculateOneWayHash(q.QueueName),
                    Scope = string.IsNullOrEmpty(q.Scope) ? q.Scope : shared.Mask(q.Scope),
                    ScopeHash = string.IsNullOrEmpty(q.Scope) ? "" : OneWayHasher.CalculateOneWayHash(q.Scope),
                    Throughput = 0
                })],
                TotalThroughput = 0,
                TotalQueues = metadata.QueueNames.Length,
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