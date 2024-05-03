namespace Particular.EndpointThroughputCounter.Data
{
    using System;
    using System.Text.Json.Serialization;

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

        public string Prefix { get; init; }

        public string ScopeType { get; init; }

        public DateTimeOffset StartTime { get; init; }

        public DateTimeOffset EndTime { get; init; }

        /// <summary>
        /// Not necessarily the difference between Start/End time. ASB for example collects
        /// 30 days worth of data and reports the max daily throughput for one 24h period.
        /// </summary>
        public TimeSpan ReportDuration { get; init; }

        public QueueThroughput[] Queues { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.Never)] // Must be serialized even if 0 to maintain compatibility with old report signatures
        public long TotalThroughput { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.Never)] // Must be serialized even if 0 to maintain compatibility with old report signatures
        public int TotalQueues { get; init; }

        public string[] IgnoredQueues { get; init; }
    }

    public class QueueThroughput
    {
        public string QueueName { get; set; }

        public long? Throughput { get; set; }

        public bool NoDataOrSendOnly { get; init; }

        public string[] EndpointIndicators { get; init; }

        public string Scope { get; init; }
    }
}