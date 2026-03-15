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

        public Task TriggerFullFlightAnalysisAsync(int masterIndex)
        {
            string requestUri = $"TelemetryAnalyzer/analyze-full-flight/{masterIndex}";

            _ = Task.Run(async () =>
            {
                try
                {
                    HttpResponseMessage response = await _httpClient.GetAsync(requestUri);
                    response.EnsureSuccessStatusCode();
                    Console.WriteLine($"[ANALYSIS DONE] Flight={masterIndex}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ANALYSIS ERROR] Flight={masterIndex} | {ex.Message}");
                }
            });

            return Task.CompletedTask;
        }
    }
}
