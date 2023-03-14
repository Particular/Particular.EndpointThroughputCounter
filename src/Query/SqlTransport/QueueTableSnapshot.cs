namespace Particular.ThroughputQuery.SqlTransport
{
    public class QueueTableSnapshot : QueueTableName
    {
        public QueueTableSnapshot(QueueTableName details)
            : base(details.DatabaseName, details.Schema, details.Name)
        {

        }

        public long? RowVersion { get; set; }
    }
}

