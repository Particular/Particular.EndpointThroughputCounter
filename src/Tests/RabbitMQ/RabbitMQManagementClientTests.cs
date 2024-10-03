namespace Particular.ThroughputQuery.RabbitMQ.Tests
{
    using System;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Approvals;
    using NUnit.Framework;
    using RabbitMQ;

    [TestFixture]
    public class RabbitMQManagementClientTests
    {
        FakeHttpHandler httpHandler;
        RabbitMQManagementClient client;

        [SetUp]
        public void Setup()
        {
            httpHandler = new FakeHttpHandler();

            client = new RabbitMQManagementClient(() => new HttpClient(httpHandler), "http://localhost:15672");
        }

        [TearDown]
        public void TearDown() => httpHandler.Dispose();
        public Func<HttpRequestMessage, HttpResponseMessage> SendCallback
        {
            get => httpHandler.SendCallback;
            set => httpHandler.SendCallback = value;
        }


        [Test]
        public void Should_handle_duplicated_json_data()
        {
            SendCallback = _ =>
            {
                var response = new HttpResponseMessage
                {
                    Content = new StringContent("""
                    {
                        "items": [
                            {
                                "name": "queue1",
                                "vhost": "vhost1",
                                "memory": 1024,
                                "memory": 1024,
                                "message_stats": {
                                    "ack": 1
                                }
                            },
                            {
                                "name": "queue2",
                                "vhost": "vhost2",
                                "vhost": "vhost2",
                                "message_stats": {
                                    "ack": 2
                                }
                            }
                        ],
                        "page": 1,
                        "page_count": 1,
                        "page_size": 500,
                        "total_count": 2
                    }
                    """)
                };
                return response;
            };

            Assert.DoesNotThrowAsync(async () => await client.GetQueueDetails());
        }

        [Test]
        public async Task Should_fetch_queue_details()
        {
            SendCallback = _ =>
            {
                var response = new HttpResponseMessage
                {
                    Content = new StringContent("""
                    {
                        "items": [
                            {
                                "name": "queue1",
                                "vhost": "vhost1",
                                "memory": 1024,
                                "message_stats": {
                                    "ack": 1
                                }
                            },
                            {
                                "name": "queue2",
                                "vhost": "vhost2",
                                "message_stats": {
                                    "ack": 2
                                }
                            },
                            {
                                "name": "queue3",
                                "vhost": "vhost1"
                            }
                        ],
                        "page": 1,
                        "page_count": 1,
                        "page_size": 500,
                        "total_count": 2
                    }
                    """)
                };
                return response;
            };

            Approver.Verify(await client.GetQueueDetails());
        }

        [Test]
        public async Task Should_fetch_queue_details_in_old_format()
        {
            SendCallback = _ =>
            {
                var response = new HttpResponseMessage
                {
                    Content = new StringContent("""
                    [
                        {
                            "name": "queue1",
                            "vhost": "vhost1",
                            "memory": 1024,
                            "message_stats": {
                                "ack": 1
                            }
                        },
                        {
                            "name": "queue2",
                            "vhost": "vhost2",
                            "message_stats": {
                                "ack": 2
                            }
                        },
                        {
                            "name": "queue3",
                            "vhost": "vhost1"
                        }
                    ]
                    """)
                };
                return response;
            };

            Approver.Verify(await client.GetQueueDetails());
        }

        [Test]
        public async Task Should_fetch_queue_details_with_paging()
        {
            SendCallback = request =>
            {
                HttpResponseMessage response = null;

                if (request.RequestUri.ToString().Contains("page=1"))
                {
                    response = new HttpResponseMessage
                    {
                        Content = new StringContent("""
                        {
                            "items": [
                                {
                                    "name": "queue1",
                                    "vhost": "vhost1",
                                    "memory": 1024,
                                    "message_stats": {
                                        "ack": 1
                                    }
                                }
                            ],
                            "page": 1,
                            "page_count": 2,
                            "page_size": 500,
                            "total_count": 2
                        }
                        """)
                    };
                }
                else if (request.RequestUri.ToString().Contains("page=2"))
                {
                    response = new HttpResponseMessage
                    {
                        Content = new StringContent("""
                        {
                            "items": [
                                {
                                    "name": "queue2",
                                    "vhost": "vhost2",
                                    "message_stats": {
                                        "ack": 2
                                    }
                                }
                            ],
                            "page": 2,
                            "page_count": 2,
                            "page_size": 500,
                            "total_count": 2
                        }
                        """)
                    };
                }
                else
                {
                    throw new Exception();
                }

                return response;
            };

            Approver.Verify(await client.GetQueueDetails());
        }

        sealed class FakeHttpHandler : HttpClientHandler
        {
            public Func<HttpRequestMessage, HttpResponseMessage> SendCallback { get; set; }

            protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken) => SendCallback(request);

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => Task.FromResult(SendCallback(request));
        }
    }
}