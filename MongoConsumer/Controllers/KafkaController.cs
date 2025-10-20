using Microsoft.AspNetCore.Mvc;
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

        [HttpGet("messages")]
        public IActionResult GetMessages()
        {
            if (!_consumerService.IsListening)
                return StatusCode(StatusCodes.Status503ServiceUnavailable,
                    "Kafka listener is not running.");

            List<object> messages = _consumerService.GetAllMessages();

            if (messages.Count == 0)
                return Ok("Kafka listener is running, but no messages have been received yet.");

            return Ok(messages);
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

            _ = _consumerService.StartListeningAsync();

            return Accepted("Kafka listener started.");
        }

    }
}
