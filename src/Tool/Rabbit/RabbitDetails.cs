class RabbitDetails
{
    public string RabbitVersion { get; set; }
    public string ManagementVersion { get; set; }
    public string ClusterName { get; set; }

    public string ToReportMethodString()
    {
        return $"RabbitMQ v{RabbitVersion}, Mgmt v{ManagementVersion}, Cluster {ClusterName}";
    }
}