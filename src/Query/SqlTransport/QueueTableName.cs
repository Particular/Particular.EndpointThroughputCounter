namespace Particular.ThroughputQuery.SqlTransport
{
    using System.Diagnostics;

    [DebuggerDisplay("{FullName}")]
    public class QueueTableName
    {
        public string DatabaseName { get; }
        public string Schema { get; }
        public string Name { get; }

        public QueueTableName(string dbName, string tableSchema, string tableName)
        {
            DatabaseName = dbName;
            Schema = tableSchema;
            Name = tableName;
        }

        public string FullName => $"[{Schema}].[{Name}]";

        public string DisplayName => $"[{DatabaseName}].{FullName}";
    }

    public class QueueTableSnapshot : QueueTableName
    {
        public QueueTableSnapshot(QueueTableName details)
            : base(details.DatabaseName, details.Schema, details.Name)
        {

        }

        public long? RowVersion { get; set; }
    }

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

