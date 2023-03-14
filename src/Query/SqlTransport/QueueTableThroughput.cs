namespace Particular.ThroughputQuery.SqlTransport
{
    public class QueueTableThroughput : QueueTableName
    {
        public QueueTableThroughput(QueueTableName table, long throughput)
            : base(table.DatabaseName, table.Schema, table.Name)
        {
            Throughput = throughput;
        }

        public long Throughput { get; }
    }
}

