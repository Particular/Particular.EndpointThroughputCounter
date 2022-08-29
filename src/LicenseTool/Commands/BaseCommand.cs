using LicenseTool.Data;
using Newtonsoft.Json;

abstract class BaseSamplingCommand<TQueueState> : BaseCommand
{
    protected abstract Task<TQueueState> SampleData(CancellationToken cancellationToken = default);

    protected abstract IEnumerable<QueueThroughput> CalculateThroughput(TQueueState start, TQueueState end);

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
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

        return new QueueDetails
        {
            Queues = queues,
            StartTime = startTime,
            EndTime = endTime
        };
    }
}

abstract class BaseCommand
{
    protected abstract Task<QueueDetails> GetData(CancellationToken cancellationToken = default);

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


}