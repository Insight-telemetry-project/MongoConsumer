using Confluent.Kafka;
using MongoConsumer.Models.Constant;
namespace MongoConsumer.Services.Kafka
{
    public class KafkaConsumerService
    {

        public async Task<List<string>> ConsumeNewMessagesAsync(int maxMessages = 5)
        {
            List<string> messages = new List<string>();

            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = ConstantKafka.KAFKA_ADDRESS,
                GroupId = ConstantKafka.GROUP_ID,
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Latest 
            };

            using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(ConstantKafka.TOPIC_NAME);

                int count = 0;

                while (count < maxMessages)
                {
                    ConsumeResult<Ignore, string> result = consumer.Consume(TimeSpan.FromSeconds(2));
                    if (result != null && result.Message != null)
                    {
                        messages.Add(result.Message.Value);
                        count++;
                    }
                }

                consumer.Close();
            }

            return messages;
        }
    }
}
