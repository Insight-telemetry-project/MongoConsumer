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

        private readonly Dictionary<int, int> _receivedFrames = new();
        private readonly Dictionary<int, int> _expectedFrames = new();
        private readonly HashSet<int> _analysisStarted = new();
        private readonly object _syncLock = new();
        private readonly IFlightAnalysisTriggerService _analysisTrigger;

        public KafkaConsumerService(
            ITelemetryRepository repository,IFlightAnalysisTriggerService analysisTrigger)
        {
            _repository = repository;
            _analysisTrigger = analysisTrigger;
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
                AutoOffsetReset = AutoOffsetReset.Latest
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
