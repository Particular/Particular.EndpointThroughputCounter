class EnvironmentDetails
{
    public string MessageTransport { get; init; }
    public string ReportMethod { get; init; }
    public string ScopeType { get; init; }
    public ScopeAndQueue[] QueueNames { get; init; }
    public string Prefix { get; init; }
    public string[] IgnoredQueues { get; init; }
    public bool SkipEndpointListCheck { get; init; }
    public bool QueuesAreEndpoints { get; init; }
}

record ScopeAndQueue(string Scope, string QueueName);