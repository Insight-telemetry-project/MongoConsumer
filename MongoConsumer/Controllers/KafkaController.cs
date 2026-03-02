using Microsoft.AspNetCore.Mvc;
using MongoConsumer.Models.Dto;
using MongoConsumer.Models.Interface;
using MongoConsumer.Services.Kafka;

namespace MongoConsumer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KafkaController : ControllerBase
    {
        private readonly IKafkaConsumerService _consumerService;

        public KafkaController(IKafkaConsumerService consumerService)
        {
            _consumerService = consumerService;
        }


        [HttpPost("stop")]
        public IActionResult StopKafkaListener()
        {
            _consumerService.StopListening();
            return Ok("Kafka listener stopped.");
        }

        [HttpPost("start")]
        public IActionResult StartKafkaListener()
        {
            if (_consumerService.IsListening)
                return Ok("Kafka listener is already running.");

            try
            {
                _consumerService.StartListeningAsync();
                return Accepted("Kafka listener started in background.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Failed to start Kafka listener");
            }
        }




        [HttpPost("update-expected-frames")]
        public IActionResult UpdateExpectedFrames([FromBody] FlightFramesUpdateRequest request)
        {
            _consumerService.UpdateExpectedFrames(request.MasterIndex, request.ExpectedFrames);

            return Ok($"Expected frames updated for flight {request.MasterIndex}");
        }


    }
}
