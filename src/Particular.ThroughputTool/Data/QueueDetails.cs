using System;
using Particular.ThroughputTool.Data;

public class QueueDetails
{
    public QueueThroughput[] Queues { get; init; }
    public DateTimeOffset StartTime { get; init; }
    public DateTimeOffset EndTime { get; init; }
}

