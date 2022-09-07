using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Particular.ThroughputTool.Data;

abstract class BaseSamplingCommand<TQueueState> : BaseCommand
{
    public BaseSamplingCommand(string outputPath, string[] maskNames)
        : base(outputPath, maskNames)
    {
    }

    protected abstract Task<TQueueState> SampleData(CancellationToken cancellationToken = default);

    protected abstract IEnumerable<QueueThroughput> CalculateThroughput(TQueueState start, TQueueState end);

    protected override async Task<QueueDetails> GetData(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Taking initial queue statistics.");
        var startData = await SampleData(cancellationToken);
        var startTime = DateTimeOffset.Now;

        Console.WriteLine("Waiting until next reading...");
        var waitUntil = DateTime.UtcNow + PollingRunTime;
        while (DateTime.UtcNow < waitUntil)
        {
            var timeLeft = waitUntil - DateTime.UtcNow;
            Console.Write($"\rWait Time Left: {timeLeft:hh':'mm':'ss}");
            await Task.Delay(250, cancellationToken);
        }

        Console.WriteLine();
        Console.WriteLine();

        Console.WriteLine("Taking final queue statistics.");
        var endData = await SampleData(cancellationToken);
        var endTime = DateTimeOffset.Now;

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
