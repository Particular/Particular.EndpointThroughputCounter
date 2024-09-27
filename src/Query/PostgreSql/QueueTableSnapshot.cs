namespace Particular.ThroughputQuery.PostgreSql
{
    public class QueueTableSnapshot : QueueTableName
    {
        public QueueTableSnapshot(QueueTableName details)
            : base(details.DatabaseName, details.Schema, details.Name)
        {
        }

        public string SequenceName => $"{FullName}_seq_seq";

        public long? RowVersion { get; set; }
    }
}