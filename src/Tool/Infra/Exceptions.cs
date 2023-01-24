using System;
using System.Collections.Generic;
using Mindscape.Raygun4Net;
using Mindscape.Raygun4Net.AspNetCore;

public static class Exceptions
{
#if RELEASE
    static RaygunClient raygun = new RaygunClient("e08ES555Pc1wZUhEQkafEQ");
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
        var settings = new RaygunSettings()
        {
            ApplicationVersion = Versioning.NuGetVersion
        };

        var ticketId = Guid.NewGuid().ToString()[..8];

        var customData = new Dictionary<string, string>()
        {
            { "TicketId", ticketId },
            { "ToolOutput", Out.GetToolOutput() }
        };

        var message = RaygunMessageBuilder.New(settings)
            .SetExceptionDetails(x)
            .SetEnvironmentDetails()
            .SetMachineName(Environment.MachineName)
            .SetUserCustomData(customData)
            .SetVersion($"{Versioning.NuGetVersion} Sha:{Versioning.FullSha}")
            .Build();

        try
        {
#if DEBUG
            Console.WriteLine(message);
#else
            raygun.Send(message).GetAwaiter().GetResult();
#endif
            Console.WriteLine($"When contacting support, you may reference TicketId: {ticketId}");
        }
        catch (Exception)
        {
            Console.WriteLine("Unable to report tool failure to Particular Software. This may be because outgoing internet access is not available.");
        }

    }
}
