using Microsoft.Data.SqlClient;
using Mindscape.Raygun4Net;
using Mindscape.Raygun4Net.AspNetCore;
using Particular.EndpointThroughputCounter.Infra;

public static class Exceptions
{
#if RELEASE
    static readonly RaygunClient raygun = new RaygunClient(new RaygunSettings
    {
        ApiKey = "e08ES555Pc1wZUhEQkafEQ"
    });
#endif

    public static void SetupUnhandledExceptionHandling()
    {
        AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
        {
            var exception = e.ExceptionObject as Exception;

            Out.WriteError(w =>
            {
                // Just to tell the difference between e.Terminating or not based on the log
                var msg = $"An unhandled exception was caught{(e.IsTerminating ? ", forcing a runtime exit" : "")}.";

                Out.WriteLine();
                Out.WriteLine(msg);
                Out.WriteLine(exception.ToString());
                Out.WriteLine();
                Out.WriteLine("Contact Particular Software support for assistance.");
            });

            ReportError(exception);

            Environment.Exit((int)HaltReason.RuntimeError);
        };
    }

    public static void ReportError(Exception x)
    {
        WriteDiagnosticsLog(x);

        var settings = new RaygunSettings()
        {
            ApplicationVersion = Versioning.NuGetVersion
        };

        RunInfo.Add("ToolOutput", Out.GetToolOutput());

        if (FindInChain<SqlException>(x) is SqlException sqlX)
        {
            RunInfo.Add("SqlException.Number", sqlX.Number.ToString());
            if (sqlX.Errors is not null)
            {
                for (var i = 0; i < sqlX.Errors.Count; i++)
                {
                    var err = sqlX.Errors[i];
                    RunInfo.Add($"SqlException.Errors${i}.Number", err.Number.ToString());
                    RunInfo.Add($"SqlException.Errors${i}.Error", err.ToString());
                }
            }
        }

        var message = RaygunMessageBuilder.New(settings)
            .SetExceptionDetails(x)
            .SetEnvironmentDetails()
            .SetMachineName(Environment.MachineName)
            .AddCurrentRunInfo()
            .SetVersion($"{Versioning.NuGetVersion} Sha:{Versioning.FullSha}")
            .Build();

        try
        {
#if DEBUG
            Console.WriteLine(message);
#else
            raygun.Send(message).GetAwaiter().GetResult();
#endif
            Console.WriteLine($"When contacting support, you may reference TicketId: {RunInfo.TicketId}");
        }
        catch (Exception)
        {
            Console.WriteLine("Unable to report tool failure to Particular Software. This may be because outgoing internet access is not available.");
        }

    }

    /// <summary>
    /// Writes the full (redacted) exception chain to a local log file so support can diagnose failures
    /// without a live debugger, including when running with --unattended.
    /// </summary>
    static void WriteDiagnosticsLog(Exception x)
    {
        try
        {
            var path = Path.Join(Environment.CurrentDirectory, $"throughput-diagnostics-{DateTime.Now:yyyyMMdd-HHmmss}.log");
            File.WriteAllText(path, ConnectionStringSanitizer.RedactText(x.ToString()));
            Console.WriteLine($"Diagnostic details written to {path}");
        }
        catch (Exception logX) when (logX is IOException or UnauthorizedAccessException)
        {
            Console.WriteLine($"Unable to write diagnostics log: {logX.Message}");
        }
    }

    static T FindInChain<T>(Exception x) where T : Exception
    {
        while (x is not null)
        {
            if (x is T match)
            {
                return match;
            }
            x = x.InnerException;
        }
        return null;
    }
}
