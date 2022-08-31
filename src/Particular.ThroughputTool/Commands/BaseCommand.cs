using System.Reflection;
using Newtonsoft.Json;
using Particular.ThroughputTool.Data;

abstract class BaseCommand
{
    readonly string outputPath;
    readonly string[] maskNames;

    public BaseCommand(string outputPath, string[] maskNames)
    {
        this.outputPath = outputPath;
        this.maskNames = maskNames;
    }

    public async Task Run(CancellationToken cancellationToken = default)
    {
        Console.Write("Enter customer name: ");
        string customerName = Console.ReadLine();
        Console.WriteLine();

        var metadata = await GetEnvironment(cancellationToken);
        var data = await GetData(cancellationToken);

        foreach (var q in data.Queues)
        {
            foreach (string mask in maskNames)
            {
                q.QueueName = q.QueueName.Replace(mask, "***", StringComparison.OrdinalIgnoreCase);
            }
        }



        var reportData = new Report
        {
            CustomerName = customerName,
            MessageTransport = metadata.MessageTransport,
            ReportMethod = metadata.ReportMethod,
            ToolVersion = Versioning.NuGetVersion,
            StartTime = data.StartTime,
            EndTime = data.EndTime,
            TestDuration = data.EndTime - data.StartTime,
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

        Console.WriteLine($"Writing report to {outputPath}");
        using (var writer = new StreamWriter(outputPath, false))
        using (var jsonWriter = new JsonTextWriter(writer))
        {
            jsonWriter.Formatting = Formatting.Indented;
            ser.Serialize(jsonWriter, report, typeof(SignedReport));
        }
    }

    protected abstract Task<QueueDetails> GetData(CancellationToken cancellationToken = default);

    protected abstract Task<EnvironmentDetails> GetEnvironment(CancellationToken cancellationToken = default);
}