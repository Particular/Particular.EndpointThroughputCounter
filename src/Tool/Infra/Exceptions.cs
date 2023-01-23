using System;

public static class Exceptions
{
    public static void SetupUnhandledExceptionHandling()
    {
        AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
        {
            Out.WriteError(w =>
            {
                // Just to tell the difference between e.Terminating or not based on the log
                var msg = $"An unhandled exception was caught{(e.IsTerminating ? ", forcing a runtime exit" : "")}.";

                Out.WriteLine();
                Out.WriteLine(msg);
                Out.WriteLine(e.ExceptionObject.ToString());
                Out.WriteLine();
                Out.WriteLine("Contact Particular Software support for assistance.");

                Environment.Exit(-3);
            });
        };
    }
}
