using System;

class HaltException : ApplicationException
{
    public int ExitCode { get; set; }

    public HaltException(HaltReason reason, string message) : base(message)
    {
        ExitCode = (int)reason;
    }
}

enum HaltReason
{
    UserCancellation = 1,
    OutputFile = 2,
    MissingConfig = 3,
    InvalidConfig = 4,
    InvalidEnvironment = 5,
    RuntimeError = 6,
}