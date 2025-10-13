using Confluent.Kafka;
using MongoConsumer.Models.Constant;
using System.Diagnostics;

namespace MongoConsumer.Services.Kafka
{
    public class KafkaConsumerService
    {
        private readonly List<string> _receivedMessages = new List<string>();
        private CancellationTokenSource? _cts;

        public async Task StartListeningAsync()
        {
            _cts = new CancellationTokenSource();
            CancellationToken token = _cts.Token;

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
                    Debug.WriteLine($"Kafka listener started on topic '{ConstantKafka.TOPIC_NAME}'.");

                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            ConsumeResult<Ignore, string>? result = consumer.Consume(TimeSpan.FromSeconds(ConstantKafka.SECONDS_TO_WAIT));
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
                    consumer.Close();
                    Debug.WriteLine("Kafka listener stopped gracefully.");
                }
            });
        }

        public void StopListening()
        {
            if (_cts != null)
            {
                Debug.WriteLine("Stopping Kafka listener...");
                _cts.Cancel();
            }
        }

        public List<string> GetAllMessages()
        {
            lock (_receivedMessages)
            {
                return new List<string>(_receivedMessages);
            }
        }
        public bool IsListening => _cts != null && !_cts.IsCancellationRequested;

    }
}
