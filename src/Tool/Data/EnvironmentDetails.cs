class EnvironmentDetails
{
    public string MessageTransport { get; init; }
    public string ReportMethod { get; init; }
    public string[] QueueNames { get; init; }
    public bool SkipEndpointListCheck { get; init; }
}