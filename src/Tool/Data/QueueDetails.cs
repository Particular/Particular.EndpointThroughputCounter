using System;
using Particular.EndpointThroughputCounter.Data;

public class QueueDetails
{
    public QueueThroughput[] Queues { get; init; }
    public DateTimeOffset StartTime { get; init; }
    public DateTimeOffset EndTime { get; init; }
    /// <summary>
    /// If reported as null, the report will assume (EndTime - StartTime)
    /// </summary>
    public TimeSpan? TimeOfObservation { get; init; }
}

