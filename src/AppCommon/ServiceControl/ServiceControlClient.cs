﻿namespace Particular.EndpointThroughputCounter.ServiceControl
{
    using System;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Particular.EndpointThroughputCounter.Infra;

    class ServiceControlClient
    {
        static readonly Version MinServiceControlVersion = new Version(4, 21, 8);

        readonly Func<HttpClient> httpFactory;
        readonly string rootUrl;
        readonly string paramName;
        readonly string instanceType;
        readonly bool tryUnsupportedVersion;

        public SemVerVersion Version { get; private set; }

        public ServiceControlClient(string paramName, string instanceType, string rootUrl, Func<HttpClient> httpFactory, bool tryUnsupportedVersion = false)
        {
            if (string.IsNullOrWhiteSpace(rootUrl))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The {paramName} option specifying the {instanceType} URL was not provided.");
            }

            this.paramName = paramName;
            this.instanceType = instanceType;
            this.rootUrl = rootUrl.TrimEnd('/');
            this.httpFactory = httpFactory;
            this.tryUnsupportedVersion = tryUnsupportedVersion;
        }

        public Task<TJsonType> GetData<TJsonType>(string pathAndQuery, CancellationToken cancellationToken = default)
        {
            return GetData<TJsonType>(pathAndQuery, 1, cancellationToken);
        }

        public string GetFullUrl(string pathAndQuery)
        {
            if (pathAndQuery is null || !pathAndQuery.StartsWith('/'))
            {
                throw new ArgumentException("pathAndQuery must start with a forward slash.");
            }

            return rootUrl + pathAndQuery;
        }

        public async Task<TJsonType> GetData<TJsonType>(string pathAndQuery, int tryCount, CancellationToken cancellationToken = default)
        {
            var url = GetFullUrl(pathAndQuery);

            using var http = httpFactory();

            for (int i = 0; i < tryCount; i++)
            {
                try
                {
                    using (var stream = await http.GetStreamAsync(url, cancellationToken))
                    {
                        return JsonSerializer.Deserialize<TJsonType>(stream);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception x)
                {
                    if (i + 1 >= tryCount)
                    {
                        throw new ServiceControlDataException(url, tryCount, x);
                    }
                }
            }

            throw new InvalidOperationException("Retry loop ended without returning or throwing. This should not happen.");
        }

        public async Task CheckEndpoint(Func<string, bool> contentTest, CancellationToken cancellationToken = default)
        {
            using var http = httpFactory();

            HttpResponseMessage res = null;
            try
            {
                res = await http.SendAsync(new HttpRequestMessage(HttpMethod.Get, rootUrl), cancellationToken);
            }
            catch (HttpRequestException hx)
            {
                throw new HaltException(HaltReason.InvalidEnvironment, $"The server at {rootUrl} did not respond. The exception message was: {hx.Message}");
            }

            if (!res.IsSuccessStatusCode)
            {
                var b = new StringBuilder($"The server at {rootUrl} returned a non-successful status code: {(int)res.StatusCode} {res.StatusCode}")
                    .AppendLine()
                    .AppendLine("Response Headers:");

                foreach (var header in res.Headers)
                {
                    _ = b.AppendLine($"  {header.Key}: {header.Value}");
                }

                throw new HaltException(HaltReason.RuntimeError, b.ToString());
            }

            if (!res.Headers.TryGetValues("X-Particular-Version", out var versionHeaders))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The server at {rootUrl} specified by parameter {paramName} does not appear to be a ServiceControl instance. Are you sure you have the right URL?");
            }

            Version = versionHeaders.Select(header => SemVerVersion.ParseOrDefault(header)).FirstOrDefault();
            Out.WriteLine($"{instanceType} instance at {rootUrl} detected running version {Version}");

            if (Version.Version < MinServiceControlVersion)
            {
                var unsupportedVersionMessage = $"The {instanceType} instance at {rootUrl} is running version {Version}. The minimum supported version is {MinServiceControlVersion.ToString(3)}.";

                var overrideUnsupportedMessage =
                    $"Overriding minimum supported version {MinServiceControlVersion.ToString(3)}. Trying to run on {instanceType} instance at {rootUrl} with version {Version}.";

                if (!tryUnsupportedVersion)
                {
                    throw new HaltException(HaltReason.InvalidEnvironment, unsupportedVersionMessage);
                }
                else
                {
                    Out.WriteWarn(overrideUnsupportedMessage);
                }
            }

            var content = await res.Content.ReadAsStringAsync(cancellationToken);
            if (!contentTest(content))
            {
                throw new HaltException(HaltReason.InvalidConfig, $"The server at {rootUrl} specified by parameter {paramName} does not appear to be a {instanceType} instance. Are you sure you have the right URL?");
            }
        }
    }
}
