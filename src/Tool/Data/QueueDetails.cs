using System;
using Particular.EndpointThroughputCounter.Data;

/// <summary>
/// Details about the Queue
/// </summary>
public class QueueDetails
{
    /// <summary>
    /// Queues
    /// </summary>
    public QueueThroughput[] Queues { get; init; }
    /// <summary>
    /// The time when the queue throughput started being measured
    /// </summary>
    public DateTimeOffset StartTime { get; init; }
    /// <summary>
    /// The time when the queue throughput stopped being measured
    /// </summary>
    public DateTimeOffset EndTime { get; init; }
    /// <summary>
    /// If reported as null, the report will assume (EndTime - StartTime)
    /// </summary>
    public TimeSpan? TimeOfObservation { get; init; }
}

