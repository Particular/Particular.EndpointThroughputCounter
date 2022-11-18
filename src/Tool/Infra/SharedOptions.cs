using System.CommandLine;
using System.CommandLine.Invocation;

class SharedOptions
{
    static readonly Option<string[]> maskNames = new("--queueNameMasks")
    {
        Description = "An optional list of strings to mask in report output to protect confidential or proprietary information",
        Arity = ArgumentArity.ZeroOrMore,
        AllowMultipleArgumentsPerToken = true
    };

    static readonly Option<string> customerName = new("--customerName")
    {
        Description = "The organization name to include in the report file. If not provided, the tool will prompt for the information."
    };

    public static readonly Option<bool> runUnattended = new("--unattended")
    {
        Description = "Allow the tool to run without user interaction, such as in a continuous integration environment.",
        Arity = ArgumentArity.ZeroOrOne
    };

    public string[] MaskNames { get; private set; }
    public string CustomerName { get; set; }
    public bool RunUnattended { get; private set; }

    public static void Register(Command command)
    {
        command.AddGlobalOption(maskNames);
        command.AddGlobalOption(customerName);
        command.AddGlobalOption(runUnattended);
    }

    public static SharedOptions Parse(InvocationContext context)
    {
        var parse = context.ParseResult;

        return new SharedOptions
        {
            MaskNames = parse.GetValueForOption(maskNames),
            CustomerName = parse.GetValueForOption(customerName),
            RunUnattended = parse.GetValueForOption(runUnattended)
        };
    }
}