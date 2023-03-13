namespace Particular.ThroughputQuery.SqlTransport
{
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    [DebuggerDisplay("{FullName}")]
    public class TableDetails
    {
        public string DatabaseName { get; }
        public string TableSchema { get; }
        public string TableName { get; }
        public long? StartRowVersion { get; set; }
        public long? EndRowVersion { get; set; }

        public TableDetails(string dbName, string tableSchema, string tableName)
        {
            DatabaseName = dbName;
            TableSchema = tableSchema;
            TableName = tableName;
        }

        public string FullName => $"[{TableSchema}].[{TableName}]";

        public string DisplayName => $"[{DatabaseName}].{FullName}";

        public async Task<long?> GetMaxRowVersion(SqlConnection conn, CancellationToken cancellationToken = default)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"select max(RowVersion) from {FullName} with (nolock);";
                var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                if (value is long rowversion)
                {
                    return rowversion;
                }
            }

            return null;
        }

        public long GetThroughput()
        {
            if (StartRowVersion is not null && EndRowVersion is not null)
            {
                return EndRowVersion.Value - StartRowVersion.Value;
            }

            // For now, not being able to detect messages probably means the true value
            // is close enough to zero that it doesn't matter.
            return 0;
        }
    }
}
