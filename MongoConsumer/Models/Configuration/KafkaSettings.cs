namespace MongoConsumer.Models.Configuration
{
    public class KafkaSettings
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; set; } = string.Empty;
    }
}
