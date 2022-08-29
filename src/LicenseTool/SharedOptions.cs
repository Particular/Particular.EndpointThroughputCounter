using System.CommandLine;

static class SharedOptions
{
    public static Option<string[]> CreateMaskNamesOption()
    {
        return new Option<string[]>(
            name: "--queueNameMasks",
            description: "An optional list of strings to mask in report output to protect confidential or proprietary information")
        {
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };
    }

    public static Option<string> CreateOutputPathOption()
    {
        return new Option<string>(
            name: "--outputPath",
            description: "The path to output the report file, should end in '.json'");
    }
}