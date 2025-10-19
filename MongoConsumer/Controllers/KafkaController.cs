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


        [HttpPost("stop")]
        public IActionResult StopKafkaListener()
        {
            _consumerService.StopListening();
            return Ok("Kafka listener stopped.");
        }
    }
}
