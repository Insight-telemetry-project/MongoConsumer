namespace MongoConsumer.Models.Interface
{
    public interface ITelemetryRepository
    {
        Task InsertFlightTelemetryAsync(string json);
    }
}
