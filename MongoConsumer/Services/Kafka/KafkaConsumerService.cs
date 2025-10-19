using Confluent.Kafka;
using MongoConsumer.Models.Constant;
using MongoConsumer.Models.Interface;
using System.Diagnostics;
using System.Text.Json;

namespace MongoConsumer.Services.Kafka
{
    public class KafkaConsumerService : IKafkaConsumerService
    {
        private readonly List<object> _receivedMessages = new List<object>();
        private CancellationTokenSource? _cts;
        private readonly ITelemetryRepository _repository;

        public KafkaConsumerService(ITelemetryRepository repository)
        {
            _repository = repository;
        }

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

            await Task.Run(async () =>
            {
                using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(ConstantKafka.TOPIC_NAME);
                    Debug.WriteLine($"Kafka listener started on topic '{ConstantKafka.TOPIC_NAME}'.");

                    while (!token.IsCancellationRequested)
                    {
                        ConsumeResult<Ignore, string>? result =
                                consumer.Consume(TimeSpan.FromSeconds(ConstantKafka.SECONDS_TO_WAIT));

                        if (result != null && result.Message != null)
                        {
                            try
                            {
                                object? jsonObject = JsonSerializer.Deserialize<object>(result.Message.Value);

                                lock (_receivedMessages)
                                {
                                    _receivedMessages.Add(jsonObject ?? result.Message.Value);
                                }

                                await _repository.InsertJsonAsync(result.Message.Value);
                                Debug.WriteLine("Message saved to MongoDB.");
                            }
                            catch (JsonException)
                            {
                                lock (_receivedMessages)
                                {
                                    _receivedMessages.Add(result.Message.Value);
                                }
                            }
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

        public List<object> GetAllMessages()
        {
            lock (_receivedMessages)
            {
                return new List<object>(_receivedMessages);
            }
        }

        public bool IsListening => _cts != null && !_cts.IsCancellationRequested;
    }
}
