namespace Particular.ThroughputTool.Data
{
    using System;

    public class SignedReport
    {
        public Report ReportData { get; init; }
        public string Signature { get; init; }
    }

    public class Report
    {
        public string CustomerName { get; init; }
        public string MessageTransport { get; init; }
        public string ReportMethod { get; init; }
        public string ToolVersion { get; init; }
        public DateTimeOffset StartTime { get; init; }
        public DateTimeOffset EndTime { get; init; }
        public TimeSpan TestDuration { get; init; }
        public QueueThroughput[] Queues { get; init; }
        public int TotalThroughput { get; init; }
        public int TotalQueues { get; init; }
    }

    public class QueueThroughput
    {
        public string QueueName { get; set; }
        public int Throughput { get; init; }
    }
}