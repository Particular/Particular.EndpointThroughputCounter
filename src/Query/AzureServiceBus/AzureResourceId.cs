namespace Particular.ThroughputQuery.AzureServiceBus;

using System;

public static class AzureResourceId
{
    public static void Parse(string resourceId,
        out (string subscriptionId, string resourceGroup, string @namespace) resourceIdSegments)
    {
        resourceIdSegments = new ValueTuple<string, string, string>(null, null, null);

        if (TryParseResourceIdSegments(resourceId, out var segments))
        {
            if (segments[0].ContainsEmptyString()
                && segments[1].ContainsSubscriptions()
                && segments[3].ContainsResourceGroups()
                && segments[5].ContainsProviders()
                && segments[6].ContainsMicrosoftServiceBus()
                && segments[7].ContainsNamespaces())
            {

                var subscription = segments[2];

                var resourceGroup = segments[4];

                var @namespace = segments[8];

                resourceIdSegments = new ValueTuple<string, string, string>(subscription, resourceGroup, @namespace);
            }
        }
        else
        {
            throw new Exception(
                "The provided --resourceId value does not look like an Azure Service Bus resourceId. A correct value should take the form '/subscriptions/{GUID}/resourceGroups/{NAME}/providers/Microsoft.ServiceBus/namespaces/{NAME}'.");
        }
    }

    static bool TryParseResourceIdSegments(string resourceId, out string[] segments)
    {
        var resourceIdSegments = resourceId.Split('/');

        if (resourceIdSegments.Length != 9)
        {
            segments = Array.Empty<string>();
            return false;
        }
        else
        {
            segments = resourceIdSegments;
            return true;
        }
    }

    static bool ContainsEmptyString(this string resourceIdSegment) => resourceIdSegment.Equals(string.Empty);

    static bool ContainsSubscriptions(this string resourceIdSegment) =>
        string.Equals(resourceIdSegment, "subscriptions", StringComparison.InvariantCultureIgnoreCase);

    static bool ContainsProviders(this string resourceIdSegment) =>
        string.Equals(resourceIdSegment, "providers", StringComparison.InvariantCultureIgnoreCase);

    static bool ContainsResourceGroups(this string resourceIdSegment) =>
        string.Equals(resourceIdSegment, "resourceGroups", StringComparison.InvariantCultureIgnoreCase);

    static bool ContainsMicrosoftServiceBus(this string resourceIdSegment) =>
        string.Equals(resourceIdSegment, "Microsoft.ServiceBus", StringComparison.InvariantCultureIgnoreCase);

    static bool ContainsNamespaces(this string resourceIdSegment) =>
        string.Equals(resourceIdSegment, "namespaces", StringComparison.InvariantCultureIgnoreCase);
}
