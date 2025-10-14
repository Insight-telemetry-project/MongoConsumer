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
                return Ok("Kafka listener is not running.");

            List<object> messages = _consumerService.GetAllMessages();

            if (messages.Count == 0)
                return Ok("No messages received yet.");

            return Ok(messages);
        }

        [HttpPost("stop")]
        public IActionResult StopKafkaListener()
        {
            _consumerService.StopListening();
            return Ok("Kafka listener stopped.");
        }
    }
}
