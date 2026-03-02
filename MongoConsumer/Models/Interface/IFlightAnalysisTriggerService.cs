namespace MongoConsumer.Models.Interface
{
    public interface IFlightAnalysisTriggerService
    {
        Task TriggerFullFlightAnalysisAsync(int masterIndex);
    }
}
