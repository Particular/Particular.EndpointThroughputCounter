namespace Particular.ThroughputQuery.AmazonSQS
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.RateLimiting;
    using System.Threading.Tasks;
    using Amazon.CloudWatch;
    using Amazon.CloudWatch.Model;
    using Amazon.SQS;
    using Amazon.SQS.Model;

    public class AwsQuery
    {
        readonly AmazonCloudWatchClient cloudWatch;
        readonly AmazonSQSClient sqs;
        readonly FixedWindowRateLimiter rateLimiter;

        public DateOnly EndDate { get; set; }
        public DateOnly StartDate { get; set; }

        public string CloudWatchRegion => cloudWatch.Config.RegionEndpoint.SystemName;
        public string SQSRegion => sqs.Config.RegionEndpoint.SystemName;

        public AwsQuery()
        {
            rateLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
            {
                AutoReplenishment = true,
                // 1/4 the AWS default quota value (400) for cloudwatch, still do 20k queues in 3 minutes
                PermitLimit = 100,
                Window = TimeSpan.FromSeconds(1),
                // Otherwise AcquireAsync() will return a lease immediately with IsAcquired = false
                QueueLimit = int.MaxValue
            });
            EndDate = DateOnly.FromDateTime(DateTime.UtcNow).AddDays(1);
            StartDate = EndDate.AddDays(-365);

            sqs = new AmazonSQSClient();
            cloudWatch = new AmazonCloudWatchClient();
        }

        public async Task<List<string>> GetQueueNames(Action<int> onProgress, CancellationToken cancellationToken = default)
        {
            var request = new ListQueuesRequest
            {
                MaxResults = 1000
            };

            var queueNames = new List<string>();

            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var response = await sqs.ListQueuesAsync(request, cancellationToken).ConfigureAwait(false);

                    if (response.QueueUrls is { Count: > 0 })
                    {
                        queueNames.AddRange(response.QueueUrls.Select(url => url.Split('/')[4]).ToArray());
                    }

                    onProgress(queueNames.Count);

                    if (response.NextToken is not null)
                    {
                        request.NextToken = response.NextToken;
                    }
                    else
                    {
                        break;
                    }
                }

                return queueNames;
            }
            catch (AmazonSQSException sqsX) when (sqsX.ErrorType == Amazon.Runtime.ErrorType.Sender)
            {
                throw new QueryException(QueryFailureReason.Auth, sqsX.Message, sqsX);
            }
        }

        public async Task<List<Datapoint>> GetMMetricsData(string queueName, CancellationToken cancellationToken = default)
        {
            var req = new GetMetricStatisticsRequest
            {
                Namespace = "AWS/SQS",
                MetricName = "NumberOfMessagesDeleted",
                StartTime = StartDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
                EndTime = EndDate.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc), // Exclusive
                Period = 24 * 60 * 60, // 1 day
                Statistics = ["Sum"],
                Dimensions = [
                    new Dimension { Name = "QueueName", Value = queueName }]
            };

            using var lease = await rateLimiter.AcquireAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            var resp = await cloudWatch.GetMetricStatisticsAsync(req, cancellationToken).ConfigureAwait(false);

            return resp.Datapoints ?? [];
        }
    }
}
