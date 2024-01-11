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

        public string DatabaseNameAndSchema => $"[{DatabaseName}].[{Schema}]";
    }
}

