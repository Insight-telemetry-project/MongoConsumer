namespace MongoConsumer.Models.Interface
{
    public interface IKafkaConsumerService
    {
        Task StartListeningAsync();
        void StopListening();
        List<object> GetAllMessages();
        bool IsListening { get; }
    }
}
