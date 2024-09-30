namespace Particular.ThroughputQuery.PostgreSql
{
    using System.Diagnostics;

    [DebuggerDisplay("{QualifiedTableName}")]
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

        public string QualifiedTableName => $"\"{Schema}\".\"{Name}\"";

        public string DisplayName => $"\"{DatabaseName}\".{QualifiedTableName}";

        public string DatabaseNameAndSchema => $"[{DatabaseName}].[{Schema}]";
    }
}

