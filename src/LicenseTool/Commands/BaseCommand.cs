

using LicenseTool.Data;
using Newtonsoft.Json;

abstract class BaseCommand<TQueueState>
{
    public async Task Run(IEnumerable<string> maskNames, CancellationToken cancellationToken = default)
    {
        Console.Write("Enter customer name: ");
        string customerName = Console.ReadLine();
        Console.WriteLine();

        Console.WriteLine("Taking initial queue statistics.");
        var startTime = DateTimeOffset.Now;
        var startData = await SampleData(cancellationToken);

        Console.WriteLine("Waiting 15 minutes for next reading...");
        var waitUntil = DateTime.UtcNow.AddMinutes(0.1);
        while (DateTime.UtcNow < waitUntil)
        {
            var timeLeft = waitUntil - DateTime.UtcNow;
            Console.Write($"\r{timeLeft:mm':'ss}");
            await Task.Delay(1000, cancellationToken);
        }

        Console.WriteLine();
        Console.WriteLine();

        Console.WriteLine("Taking final queue statistics.");
        var endTime = DateTimeOffset.Now;
        var endData = await SampleData(cancellationToken);

        var queues = CalculateThroughput(startData, endData)
            .OrderBy(q => q.QueueName)
            .ToArray();

        foreach (var q in queues)
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
            StartTime = startTime,
            EndTime = endTime,
            TestDuration = endTime - startTime,
            Queues = queues,
            TotalThroughput = queues.Sum(q => q.Throughput),
            TotalQueues = queues.Length
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

    protected abstract Task<TQueueState> SampleData(CancellationToken cancellationToken = default);

    protected abstract IEnumerable<QueueThroughput> CalculateThroughput(TQueueState start, TQueueState end);
}