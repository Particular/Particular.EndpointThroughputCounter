using LicenseTool.Data;
using Newtonsoft.Json;

abstract class BaseCommand
{
    public async Task Run(IEnumerable<string> maskNames, CancellationToken cancellationToken = default)
    {
        Console.Write("Enter customer name: ");
        string customerName = Console.ReadLine();
        Console.WriteLine();

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
            MessageTransport = "RabbitMQ",
            ReportMethod = "LicenseTool: RabbitMQ Admin",
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

        using (var writer = new StreamWriter(@"H:\licensetool\report.json", false))
        using (var jsonWriter = new JsonTextWriter(writer))
        {
            jsonWriter.Formatting = Formatting.Indented;
            ser.Serialize(jsonWriter, report, typeof(SignedReport));
        }
    }

    protected virtual Task Initialize(CancellationToken cancellationToken = default) => Task.CompletedTask;

    protected abstract Task<QueueDetails> GetData(CancellationToken cancellationToken = default);
}