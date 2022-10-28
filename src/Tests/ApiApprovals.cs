namespace Tests;

using NUnit.Framework;
using Particular.Approvals;
using Particular.EndpointThroughputCounter.Data;
using PublicApiGenerator;

[TestFixture]
public class APIApprovals
{
    [Test]
    public void Approve()
    {
        var publicApi = typeof(Report).Assembly.GeneratePublicApi(new ApiGeneratorOptions
        {
            ExcludeAttributes = new[] { "System.Runtime.Versioning.TargetFrameworkAttribute", "System.Reflection.AssemblyMetadataAttribute" }
        });
        Approver.Verify(publicApi);
    }
}