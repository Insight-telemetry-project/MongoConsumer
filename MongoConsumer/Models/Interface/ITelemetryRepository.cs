namespace MongoConsumer.Models.Interface
{
    public interface ITelemetryRepository
    {
        Task InsertJsonAsync(string json);
    }
}
