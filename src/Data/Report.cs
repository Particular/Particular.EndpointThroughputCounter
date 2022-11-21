namespace Particular.EndpointThroughputCounter.Data
{
    using System;
    using Newtonsoft.Json;

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

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string Prefix { get; init; }

        public DateTimeOffset StartTime { get; init; }

        public DateTimeOffset EndTime { get; init; }

        /// <summary>
        /// Not necessarily the difference between Start/End time. ASB for example collects
        /// 30 days worth of data and reports the max daily throughput for one 24h period.
        /// </summary>
        public TimeSpan ReportDuration { get; init; }

        public QueueThroughput[] Queues { get; init; }

        public int TotalThroughput { get; init; }

        public int TotalQueues { get; init; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string[] IgnoredQueues { get; init; }
    }

    public class QueueThroughput
    {
        public string QueueName { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int? Throughput { get; init; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool NoDataOrSendOnly { get; init; }
    }
}