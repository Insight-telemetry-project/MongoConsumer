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
            lock (_receivedMessages)
            {
                _receivedMessages.Clear();
            }
            _cts = new CancellationTokenSource();
            CancellationToken token = _cts.Token;

            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = ConstantKafka.KAFKA_ADDRESS,
                GroupId = ConstantKafka.GROUP_ID,
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await RunKafkaListenerAsync(config, token);
        }

        public void StopListening()
        {
            if (_cts != null)
            {
                _cts.Cancel();
            }
        }

        private Task RunKafkaListenerAsync(ConsumerConfig config, CancellationToken token)
        {
            return Task.Run(() => ListenLoopAsync(config, token));
        }

        private async Task ListenLoopAsync(ConsumerConfig config, CancellationToken token)
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(ConstantKafka.TOPIC_NAME);

                while (!token.IsCancellationRequested)
                {
                    var result =
                            consumer.Consume(TimeSpan.FromSeconds(ConstantKafka.SECONDS_TO_WAIT));

                    if (result != null && result.Message != null)
                    {
                        await HandleMessageAsync(result.Message.Value);
                    }
                }

                consumer.Close();
            }
        }
        private async Task HandleMessageAsync(string message)
        {
            await _repository.InsertFlightTelemetryAsync(message);
        }


        public bool IsListening => _cts != null && !_cts.IsCancellationRequested;
    }
}
