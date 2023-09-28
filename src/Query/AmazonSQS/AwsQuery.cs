namespace Particular.ThroughputQuery.AmazonSQS
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.CloudWatch;
    using Amazon.CloudWatch.Model;
    using Amazon.SQS;
    using Amazon.SQS.Model;

    public class AwsQuery
    {
        readonly AmazonCloudWatchClient cloudWatch;
        readonly AmazonSQSClient sqs;

        public DateTime EndTimeUtc { get; set; }
        public DateTime StartTimeUtc { get; set; }

        public string CloudWatchRegion => cloudWatch.Config.RegionEndpoint.SystemName;
        public string SQSRegion => sqs.Config.RegionEndpoint.SystemName;

        public AwsQuery()
        {
            EndTimeUtc = DateTime.UtcNow.Date.AddDays(1);
            StartTimeUtc = EndTimeUtc.AddDays(-30);

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

                    queueNames.AddRange(response.QueueUrls.Select(url => url.Split('/')[4]));

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

        public async Task<long> GetMaxThroughput(string queueName, CancellationToken cancellationToken = default)
        {
            var req = new GetMetricStatisticsRequest
            {
                Namespace = "AWS/SQS",
                MetricName = "NumberOfMessagesDeleted",
                StartTimeUtc = StartTimeUtc,
                EndTimeUtc = EndTimeUtc,
                Period = 86400, // 1 day
                Statistics = new List<string> { "Sum" },
                Dimensions = new List<Dimension> {
                    new Dimension { Name = "QueueName", Value = queueName }
                }
            };

            var resp = await cloudWatch.GetMetricStatisticsAsync(req, cancellationToken).ConfigureAwait(false);

            var maxThroughput = resp.Datapoints.MaxBy(d => d.Sum)?.Sum ?? 0;

            return (long)maxThroughput;
        }
    }
}
