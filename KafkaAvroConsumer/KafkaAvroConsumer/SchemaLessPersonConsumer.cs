using com.example;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace KafkaAvroConsumer
{
    public class SchemaLessPersonConsumer
    {
        private readonly ILogger _logger;

        [Function(nameof(RunWithBytesAsync))]
        public static async Task RunWithBytesAsync(
        [KafkaTrigger("%BootstrapServers%",
                  "%Topic%",
                  Username = "%SaslUsername%",
                  Password = "%SaslPassword%",
                  Protocol = BrokerProtocol.SaslSsl,
                  AuthenticationMode = BrokerAuthenticationMode.Plain,
                  IsBatched = true,
                  ConsumerGroup = "$Default")] byte[][] data, FunctionContext context)
        {
            var logger = context.GetLogger("KafkaFunction");

            foreach (var eventData in data)
            {
                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    // Note: you can specify more than one schema registry url using the
                    // schema.registry.url property for redundancy (comma separated list).
                    Url = Environment.GetEnvironmentVariable("SchemaRegistryUrl"),
                    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                    BasicAuthUserInfo = $"{Environment.GetEnvironmentVariable("SchemaRegistryApiKey")}:{Environment.GetEnvironmentVariable("SchemaRegistryApiSecret")}"
                };

                using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
                var deserializer = new AvroDeserializer<ExtraOrdinaryPerson>(schemaRegistry);
                var person = await deserializer.DeserializeAsync(eventData, false, 
                    new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, Environment.GetEnvironmentVariable("Topic")));

                logger.LogInformation($"C# Kafka trigger function processed a message: {person}");
            }
        }
    }
}