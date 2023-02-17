namespace Particular.EndpointThroughputCounter.Infra
{
    using System;
    using System.Collections.Generic;
    using Mindscape.Raygun4Net;

    public static class RunInfo
    {
        static readonly Dictionary<string, string> runValues = new();

        public static readonly string TicketId;

        static RunInfo()
        {
            TicketId = Guid.NewGuid().ToString()[..8];
            runValues.Add("TicketId", TicketId);
        }

        public static void Add(string key, string value)
        {
            runValues[key] = value;
        }

        public static IRaygunMessageBuilder AddCurrentRunInfo(this IRaygunMessageBuilder builder)
        {
            return builder.SetUserCustomData(runValues);
        }
    }
}
