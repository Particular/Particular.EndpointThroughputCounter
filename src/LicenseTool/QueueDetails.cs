using LicenseTool.Data;

public class QueueDetails
{
    public QueueThroughput[] Queues { get; init; }
    public DateTimeOffset StartTime { get; init; }
    public DateTimeOffset EndTime { get; init; }
    public string Transport { get; init; }
    public string ReportMethod { get; init; }
}

