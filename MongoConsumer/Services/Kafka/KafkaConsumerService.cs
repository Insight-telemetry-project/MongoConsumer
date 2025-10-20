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

            await Task.Run(() => RunKafkaListener(config, token));

        }

        public void StopListening()
        {
            if (_cts != null)
            {
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


        private void RunKafkaListener(ConsumerConfig config, CancellationToken token)
        {
            using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(ConstantKafka.TOPIC_NAME);

                while (!token.IsCancellationRequested)
                {
                    ConsumeAndProcessMessage(consumer);
                }

                consumer.Close();
            }
        }
        private void ConsumeAndProcessMessage(IConsumer<Ignore, string> consumer)
        {
            ConsumeResult<Ignore, string>? result =
                consumer.Consume(TimeSpan.FromSeconds(ConstantKafka.SECONDS_TO_WAIT));

            ProcessMessage(result.Message.Value);
        }
        private void ProcessMessage(string messageValue)
        {
            try
            {
                object? jsonObject = JsonSerializer.Deserialize<object>(messageValue);
                AddMessage(jsonObject ?? messageValue);
            }
            catch (JsonException)
            {
                AddMessage(messageValue);
            }
        }
        private void AddMessage(object message)
        {
            lock (_receivedMessages)
            {
                _receivedMessages.Add(message);
            }
        }



        public bool IsListening => _cts != null && !_cts.IsCancellationRequested;
    }
}
