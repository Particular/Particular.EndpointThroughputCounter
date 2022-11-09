using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
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
        Console.WriteLine($"Particular.EndpointThroughputCounter {NuGetVersion} (Sha:{ShortSha})");

        var logger = NullLogger.Instance;
        var cache = new SourceCacheContext();
        var packageSource = new PackageSource("https://www.myget.org/F/particular/api/v3/index.json");
        var repository = new SourceRepository(packageSource, Repository.Provider.GetCoreV3());

        try
        {
            Console.WriteLine("Checking for latest version...");
            var resource = await repository.GetResourceAsync<FindPackageByIdResource>(cancellationToken);

            NuGetVersion[] versions = null;

            using (var tokenSource = new CancellationTokenSource(10_000))
            {
                try
                {
                    versions = (await resource.GetAllVersionsAsync("Particular.EndpointThroughputCounter", cache, logger, tokenSource.Token)).ToArray();
                }
                catch (OperationCanceledException) when (tokenSource.Token.IsCancellationRequested)
                {
                    Console.Error.WriteLine("WARNING: Unable to connect to MyGet within 10s timeout. The tool will still run, but only the most recent version of the tool should be used.");
                    return true;
                }
            }

            var latest = versions.OrderByDescending(pkg => pkg.Version).FirstOrDefault();
            var current = new NuGetVersion(NuGetVersion);

            if (latest != null && latest > current)
            {
                Console.WriteLine();
                Console.WriteLine($"** New version detected: {latest.ToNormalizedString()}");
#if WINX64
                Console.WriteLine("** Download the latest version here: https://s3.amazonaws.com/particular.downloads/EndpointThroughputCounter/Particular.EndpointThroughputCounter.zip");
#else
                Console.WriteLine("** To install, execute the following command:");
                Console.WriteLine(" > dotnet tool update -g Particular.EndpointThroughputCounter --add-source=https://www.myget.org/F/particular/api/v3/index.json");
#endif
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