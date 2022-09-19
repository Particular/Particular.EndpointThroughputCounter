namespace Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.Approvals;
    using Particular.EndpointThroughputCounter.Data;

    [TestFixture]
    public class SignatureTests
    {
        [Test]
        public void SignatureRoundTrip()
        {
            var report = CreateReport();

            var reportString = SerializeReport(report);
            Approver.Verify(reportString,
                scrubber: input => input.Replace(report.Signature, "SIGNATURE"),
                scenario: "Serialized");

            var deserialized = DeserializeReport(reportString);
            Approver.Verify(reportString,
                scrubber: input => input.Replace(report.Signature, "SIGNATURE"),
                scenario: "Deserialized");

            // We don't distribute the private key to do local testing, this only happens during CI
            if (Environment.GetEnvironmentVariable("CI") != "true")
            {
                return;
            }

            Assert.IsTrue(ValidateReport(deserialized));
        }

        [Test]
        public void TamperCheck()
        {
            var report = CreateReport();
            var reportString = SerializeReport(report);

            reportString = reportString.Replace("42", "13");

            var deserialized = DeserializeReport(reportString);

            // We don't distribute the private key to do local testing, this only happens during CI
            if (Environment.GetEnvironmentVariable("CI") != "true")
            {
                return;
            }

            Assert.IsFalse(ValidateReport(deserialized));
        }

        SignedReport CreateReport()
        {
            var start = new DateTimeOffset(2022, 09, 01, 0, 0, 0, TimeSpan.Zero);
            var end = start.AddDays(1);

            var queues = new[]
            {
                new QueueThroughput { QueueName = "Queue1", Throughput = 42 },
                new QueueThroughput { QueueName = "Queue1", Throughput = 10000 },
            };

            var reportData = new Report
            {
                CustomerName = "Test",
                MessageTransport = "Fake Transport",
                ReportMethod = "Testing",
                ToolVersion = "1.0.0",
                StartTime = start,
                EndTime = end,
                ReportDuration = end - start,
                Queues = queues,
                TotalThroughput = queues.Sum(q => q.Throughput),
                TotalQueues = queues.Length
            };

            return new SignedReport
            {
                ReportData = reportData,
                Signature = Signature.SignReport(reportData)
            };
        }

        string SerializeReport(SignedReport report)
        {
            var serializer = new JsonSerializer();

            using (var writer = new StringWriter())
            using (var jsonWriter = new JsonTextWriter(writer))
            {
                jsonWriter.Formatting = Formatting.Indented;
                serializer.Serialize(jsonWriter, report, typeof(SignedReport));
                return writer.ToString();
            }
        }

        SignedReport DeserializeReport(string reportString)
        {
            var serializer = new JsonSerializer();

            using (var reader = new StringReader(reportString))
            using (var jsonReader = new JsonTextReader(reader))
            {
                return serializer.Deserialize<SignedReport>(jsonReader);
            }
        }

        bool ValidateReport(SignedReport signedReport)
        {
            if (signedReport.Signature is null)
            {
                return false;
            }

            var reserializedReportBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(signedReport.ReportData, Formatting.None));

            using (var rsa = RSA.Create())
            using (var sha = SHA512.Create())
            {
                ImportPrivateKey(rsa, Environment.GetEnvironmentVariable("RSA_PRIVATE_KEY"));

                var correctSignature = Convert.ToBase64String(sha.ComputeHash(reserializedReportBytes));

                var decryptedHash = rsa.Decrypt(Convert.FromBase64String(signedReport.Signature), RSAEncryptionPadding.Pkcs1);
                var decryptedSignature = Convert.ToBase64String(decryptedHash);

                return correctSignature == decryptedSignature;
            }
        }

        static void ImportPrivateKey(RSA rsa, string privateKeyText)
        {
#if NET5_0_OR_GREATER
            rsa.ImportFromPem(privateKeyText);
#else
            var base64Builder = new StringBuilder();
            using (var reader = new StringReader(privateKeyText))
            {
                while (true)
                {
                    var line = reader.ReadLine();
                    if (line == null)
                    {
                        break;
                    }
                    if (line.StartsWith("---"))
                    {
                        continue;
                    }
                    base64Builder.Append(line.Trim());
                }
            }

            var bytes = Convert.FromBase64String(base64Builder.ToString());

            rsa.ImportRSAPrivateKey(bytes, out int bytesRead);

            if (bytesRead != bytes.Length)
            {
                throw new Exception("Failed to read public key.");
            }
#endif
        }
    }
}