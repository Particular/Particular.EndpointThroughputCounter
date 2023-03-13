namespace Particular.ThroughputQuery.SqlTransport
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    [DebuggerDisplay("{FullName}")]
    public class TableDetails
    {
        public string TableSchema { get; init; }
        public string TableName { get; init; }
        public long? StartRowVersion { get; set; }
        public long? EndRowVersion { get; set; }
        public DatabaseDetails Database { get; set; }

        public string FullName => $"[{TableSchema}].[{TableName}]";

        public string DisplayName => Database is null ? FullName : $"[{Database.DatabaseName}].{FullName}";

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

        internal bool IgnoreTable()
        {
            if (TableName is "error" or "audit")
            {
                return true;
            }

            if (TableName.StartsWith("Particular.", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
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
