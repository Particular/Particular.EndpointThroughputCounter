using LicenseTool.Data;

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
