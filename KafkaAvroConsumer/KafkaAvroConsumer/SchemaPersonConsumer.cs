using com.example;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaAvroConsumer
{
    public class SchemaPersonConsumer
    {
        const string ExtraOrdinaryPersonSchema = @"{
  ""type"": ""record"",
  ""name"": ""ExtraOrdinaryPerson"",
  ""namespace"": ""com.example"",
  ""fields"": [
    {
      ""name"": ""Id"",
      ""type"": ""int""
    },
    {
      ""name"": ""Name"",
      ""type"": ""string""
    },
    {
      ""name"": ""Age"",
      ""type"": ""int""
    },
{
      ""name"": ""Email"",
      ""type"": ""string""
    },
{
      ""name"": ""Address"",
      ""type"": ""string""
    }
  ]
}";

        [Function(nameof(RunWithStrings))]
        public static void RunWithStrings(
        [KafkaTrigger("%BootstrapServers%",
                  "%Topic%",
                  Username = "%SaslUsername%",
                  Password = "%SaslPassword%",
                  Protocol = BrokerProtocol.SaslSsl,
                  AuthenticationMode = BrokerAuthenticationMode.Plain,
                  IsBatched = true,
                  AvroSchema = ExtraOrdinaryPersonSchema,
                  SchemaRegistryUrl = "%SchemaRegistryUrl%",
                  SchemaRegistryUsername = "%SchemaRegistryApiKey%",
                  SchemaRegistryPassword = "%SchemaRegistryApiSecret%",
                  ConsumerGroup = "$Default")] string[] events, FunctionContext context)
        {
            var logger = context.GetLogger(nameof(RunWithStrings));

            foreach (var eventData in events)
            {
                var person = JsonConvert.DeserializeObject<ExtraOrdinaryPerson>(eventData);

                logger.LogInformation($"C# Kafka trigger function processed a message: {person}");
            }
        }
    }
}