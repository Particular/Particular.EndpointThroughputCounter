﻿using System.Reflection;
using System.Text.RegularExpressions;
using NuGet.Common;
using NuGet.Configuration;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;

static class Versioning
{
    public static string InformationalVersion { get; }
    public static string FullSha { get; }
    public static string ShortSha { get; }
    public static string NuGetVersion { get; }
    public static string PreReleaseLabel { get; }

    static Versioning()
    {
        var assembly = typeof(Versioning).Assembly;
        var infoVersionAtt = assembly.GetCustomAttributes<AssemblyInformationalVersionAttribute>().FirstOrDefault();
        InformationalVersion = infoVersionAtt?.InformationalVersion ?? "Unknown";

        var versionRegex = new Regex(@"^(?<CoreVersion>\d+\.\d+\.\d+)(-(?<PrereleaseLabel>[a-z0-9-]+)\.(?<PrereleaseNumber>\d+)\.(?<Height>\d+))?\+(?<FullSha>[0-9a-f]{40})$");

        var match = versionRegex.Match(InformationalVersion);
        if (match.Success)
        {
            FullSha = match.Groups["FullSha"].Value;
            ShortSha = FullSha.Substring(0, 7);

            var coreVersion = match.Groups["CoreVersion"].Value;
            PreReleaseLabel = match.Groups["PrereleaseLabel"].Value;
            if (string.IsNullOrEmpty(PreReleaseLabel))
            {
                NuGetVersion = coreVersion;
            }
            else
            {
                var prereleaseNumber = match.Groups["PrereleaseNumber"].Value;
                var height = match.Groups["Height"];
                NuGetVersion = $"{coreVersion}-{PreReleaseLabel}.{prereleaseNumber}+{height}";
            }
        }

    }

    public static async Task<bool> CheckForCurrentVersion(CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"Particular.ThroughputTool {NuGetVersion} (Sha:{ShortSha})");

        var logger = NullLogger.Instance;
        var cache = new SourceCacheContext();
        var packageSource = new PackageSource("https://www.myget.org/F/particular/api/v3/index.json");
        var repository = new SourceRepository(packageSource, Repository.Provider.GetCoreV3());

        try
        {
            Console.WriteLine("Checking for latest version...");
            var resource = await repository.GetResourceAsync<FindPackageByIdResource>(cancellationToken);
            var versions = await resource.GetAllVersionsAsync("Particular.ThroughputTool", cache, logger, cancellationToken);

            var latest = versions.OrderByDescending(pkg => pkg.Version).FirstOrDefault();
            var current = new NuGetVersion(NuGetVersion);

            if (latest != null && latest > current)
            {
                Console.WriteLine();
                Console.WriteLine($"** New version detected: {latest.ToNormalizedString()}");
                Console.WriteLine("** To install, execute the following command:");
                Console.WriteLine(" > dotnet tool update -g Particular.ThroughputTool --add-source=https://www.myget.org/F/particular/api/v3/index.json");
                Console.WriteLine();
                return false;
            }
        }
        catch (NuGetProtocolException)
        {
            Console.Error.WriteLine("WARNING: Unable to connect to www.myget.org to validate the latest version of the tool. The tool will still run, but only the most recent version of the tool should be used.");
        }

        return true;
    }
}