using Confluent.Kafka;
using Microsoft.Extensions.Options;
using MongoConsumer.Models.Configuration;
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
        private readonly string _bootstrapServers;

        private readonly Dictionary<int, int> _receivedFrames = new();
        private readonly Dictionary<int, int> _expectedFrames = new();
        private readonly HashSet<int> _analysisStarted = new();
        private readonly object _syncLock = new();
        private readonly IFlightAnalysisTriggerService _analysisTrigger;

        public KafkaConsumerService(
            ITelemetryRepository repository,
            IFlightAnalysisTriggerService analysisTrigger,
            IOptions<KafkaSettings> kafkaSettings)
        {
            _repository = repository;
            _analysisTrigger = analysisTrigger;
            _bootstrapServers = kafkaSettings.Value.BootstrapServers;
            Console.WriteLine("Kafka from ENV: " + _bootstrapServers);
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
                BootstrapServers = _bootstrapServers,
                GroupId = ConstantKafka.GROUP_ID,
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            Console.WriteLine(config.BootstrapServers);
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
            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(ConstantKafka.TOPIC_NAME);

                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(TimeSpan.FromSeconds(ConstantKafka.SECONDS_TO_WAIT));

                            if (result != null && result.Message != null)
                            {
                                await HandleMessageAsync(result.Message.Value);
                            }
                        }
                        catch (ConsumeException ex)
                        {
                            Console.WriteLine($"[KAFKA] Consume error: {ex.Error.Reason}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[KAFKA] Handler error: {ex.Message}");
                        }
                    }

                    consumer.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[KAFKA] Consumer loop fatal error: {ex}");
            }
        }
        private async Task HandleMessageAsync(string message)
        {
            await _repository.InsertFlightTelemetryAsync(message);

            int masterIndex = ExtractMasterIndex(message);

            lock (_syncLock)
            {
                if (!_receivedFrames.ContainsKey(masterIndex))
                    _receivedFrames[masterIndex] = 0;

                _receivedFrames[masterIndex]++;
            }

            await CheckIfFlightCompletedAsync(masterIndex);
        }
        public void UpdateExpectedFrames(int masterIndex, int expectedFrames)
        {
            lock (_syncLock)
            {
                _expectedFrames[masterIndex] = expectedFrames;
            }

            _ = CheckIfFlightCompletedAsync(masterIndex);
        }

        private async Task CheckIfFlightCompletedAsync(int masterIndex)
        {
            bool shouldTriggerAnalysis = false;

            lock (_syncLock)
            {
                if (!_expectedFrames.ContainsKey(masterIndex))
                    return;

                if (!_receivedFrames.ContainsKey(masterIndex))
                    return;

                int received = _receivedFrames[masterIndex];
                int expected = _expectedFrames[masterIndex];

                System.Diagnostics.Debug.WriteLine(
                    $"Flight {masterIndex} → Received: {received} / Expected: {expected}");

                if (received >= expected && !_analysisStarted.Contains(masterIndex))
                {
                    _analysisStarted.Add(masterIndex);
                    shouldTriggerAnalysis = true;
                }
            }

            if (shouldTriggerAnalysis)
            {
                await _analysisTrigger.TriggerFullFlightAnalysisAsync(masterIndex);


                lock (_syncLock)
                {
                    _receivedFrames.Remove(masterIndex);
                    _expectedFrames.Remove(masterIndex);
                    _analysisStarted.Remove(masterIndex);
                }

                System.Diagnostics.Debug.WriteLine(
                    $"Flight {masterIndex} counters cleared from memory.");
            }
        }

        private int ExtractMasterIndex(string message)
        {
            using JsonDocument document = JsonDocument.Parse(message);

            JsonElement root = document.RootElement;

            if (root.TryGetProperty("Fields", out JsonElement fieldsElement) &&
                fieldsElement.TryGetProperty("Master Index", out JsonElement masterIndexElement))
            {
                return masterIndexElement.GetInt32();
            }

            throw new InvalidOperationException("Master Index not found in message.");
        }
        public bool IsListening => _cts != null && !_cts.IsCancellationRequested;
    }
}
