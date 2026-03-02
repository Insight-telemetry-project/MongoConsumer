using MongoConsumer.Models.Interface;

namespace MongoConsumer.Services.Network
{
    public class FlightAnalysisTriggerService : IFlightAnalysisTriggerService
    {
        private readonly HttpClient _httpClient;

        public FlightAnalysisTriggerService(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public async Task TriggerFullFlightAnalysisAsync(int masterIndex)
        {
            string requestUri = $"TelemetryAnalyzer/analyze-full-flight/{masterIndex}";

            HttpResponseMessage response = await _httpClient.GetAsync(requestUri);

            response.EnsureSuccessStatusCode();
        }
    }
}
