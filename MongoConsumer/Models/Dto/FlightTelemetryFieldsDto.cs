using System.Text.Json.Serialization;

namespace MongoConsumer.Models.Dto
{
    public class FlightTelemetryFieldsDto
    {
        [JsonPropertyName("Fields")]
        public Dictionary<string, double> Fields { get; set; } = new();
    }
}
