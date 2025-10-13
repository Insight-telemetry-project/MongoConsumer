using Confluent.Kafka;
using MongoConsumer.Models.Constant;
using System.Diagnostics;

namespace MongoConsumer.Services.Kafka
{
    public class KafkaConsumerService
    {
        private readonly List<string> _receivedMessages = new List<string>();
        private bool _isListening = false;

        public async Task StartListeningAsync()
        {
            if (_isListening)
                return;

            _isListening = true;

            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = ConstantKafka.KAFKA_ADDRESS,
                GroupId = ConstantKafka.GROUP_ID,
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await Task.Run(() =>
            {
                using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(ConstantKafka.TOPIC_NAME);

                    while (true)
                    {
                        try
                        {
                            ConsumeResult<Ignore, string>? result = consumer.Consume(TimeSpan.FromSeconds(2));
                            if (result != null && result.Message != null)
                            {
                                lock (_receivedMessages)
                                {
                                    _receivedMessages.Add(result.Message.Value);
                                }
                            }
                        }
                        catch (ConsumeException ex)
                        {
                            Debug.WriteLine($"Kafka error: {ex.Error.Reason}");
                        }
                    }
                }
            });
        }

        public List<string> GetAllMessages()
        {
            lock (_receivedMessages)
            {
                return new List<string>(_receivedMessages);
            }
        }
    }
}
