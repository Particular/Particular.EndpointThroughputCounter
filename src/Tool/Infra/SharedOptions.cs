using System.CommandLine;

static class SharedOptions
{
    public static readonly Option<string[]> MaskNames = new Option<string[]>("--queueNameMasks")
    {
        Description = "An optional list of strings to mask in report output to protect confidential or proprietary information",
        Arity = ArgumentArity.ZeroOrMore,
        AllowMultipleArgumentsPerToken = true
    };
}