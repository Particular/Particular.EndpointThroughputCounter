using System;

class HaltException : ApplicationException
{
    public int ExitCode { get; set; }

    public HaltException(int exitCode, string message) : base(message)
    {
        ExitCode = exitCode;
    }
}