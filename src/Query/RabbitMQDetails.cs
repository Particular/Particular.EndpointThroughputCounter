namespace Particular.ThroughputQuery
{
    public class RabbitMQDetails
    {
        public string RabbitMQVersion { get; set; }
        public string ManagementVersion { get; set; }
        public string ClusterName { get; set; }

        public string ToReportMethodString()
        {
            return $"RabbitMQ v{RabbitMQVersion}, Mgmt v{ManagementVersion}, Cluster {ClusterName}";
        }
    }
}