using System;
using Particular.EndpointThroughputCounter.Data;

public class QueueDetails
{
    public QueueThroughput[] Queues { get; init; }
    public DateTimeOffset StartTime { get; init; }
    public DateTimeOffset EndTime { get; init; }
}

