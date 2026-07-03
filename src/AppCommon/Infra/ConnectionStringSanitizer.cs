using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

static partial class ConnectionStringSanitizer
{
    const string Mask = "*****";

    [GeneratedRegex("""(?<key>\b(?:password|pwd))\s*=\s*(?:'[^']*'|"[^"]*"|[^;]*)""", RegexOptions.IgnoreCase)]
    private static partial Regex SecretRegex();

    /// <summary>
    /// Returns the connection string with the Password/PWD value blanked so it can be safely echoed or logged.
    /// </summary>
    public static string Sanitize(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder { ConnectionString = connectionString };
            if (!string.IsNullOrEmpty(builder.Password))
            {
                builder.Password = Mask;
            }
            return builder.ToString();
        }
        catch (Exception x) when (x is FormatException or ArgumentException)
        {
            // Not parseable as a connection string, fall back to pattern-based redaction
            return RedactText(connectionString);
        }
    }

    /// <summary>
    /// Redacts Password/PWD values in free-form text, such as an exception dump that may embed a connection string.
    /// </summary>
    public static string RedactText(string text) => text is null ? null : SecretRegex().Replace(text, $"${{key}}={Mask}");
}
