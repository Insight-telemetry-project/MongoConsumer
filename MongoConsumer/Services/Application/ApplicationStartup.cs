using Microsoft.Extensions.Hosting;
using MongoConsumer.Services.Kafka;
using System.Diagnostics;

namespace MongoConsumer.Services.Application
{
    public class ApplicationStartup
    {
        private readonly KafkaConsumerService _kafkaConsumerService;
        private readonly IHostApplicationLifetime _lifetime;

        public ApplicationStartup(
            KafkaConsumerService kafkaConsumerService,
            IHostApplicationLifetime lifetime)
        {
            _kafkaConsumerService = kafkaConsumerService;
            _lifetime = lifetime;
        }
        public void RegisterApplicationEvents()
        {
            _lifetime.ApplicationStarted.Register(OnApplicationStarted);
            _lifetime.ApplicationStopping.Register(OnApplicationStopping);
        }
        private void OnApplicationStarted()
        {
            Debug.WriteLine("Application has started. Launching Kafka listener...");

            Task.Run(async () =>
            {
                try
                {
                    await _kafkaConsumerService.StartListeningAsync();
                }
                catch (Exception exception)
                {
                    Debug.WriteLine($"Kafka listener failed to start: {exception.Message}");
                }
            });
        }

        private void OnApplicationStopping()
        {
            Debug.WriteLine("Application is stopping. Cleaning up Kafka listener...");
        }
    }
}
