namespace MongoConsumer.Models.Interface
{
    public interface IKafkaConsumerService
    {
        Task StartListeningAsync();
        void StopListening();
        bool IsListening { get; }
    }
}
