using Microsoft.Extensions.Hosting;
using MongoConsumer.Models.Interface;
using MongoConsumer.Services.Kafka;
using System.Diagnostics;

namespace MongoConsumer.Services.Application
{
    public class ApplicationStartup
    {
        private readonly IKafkaConsumerService _kafkaConsumerService;
        private readonly IHostApplicationLifetime _lifetime;

        public ApplicationStartup(
            IKafkaConsumerService kafkaConsumerService,
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

            Task.Run(async () =>
            {
                await _kafkaConsumerService.StartListeningAsync();
            });
        }

        private void OnApplicationStopping()
        {
            _kafkaConsumerService.StopListening();

        }
    }
}
