using System;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaProducer
{
    class Program
    {
        private static async Task DeleteTopics(string brokerList, string[] topicNames)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerList }).Build())
            {
                await adminClient.DeleteTopicsAsync(topicNames, null);
            }
        }
        private static async Task GetStockResponse()
        {
            using (var httpClient = new HttpClient())
            {
                string apiUri = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=PRGS&interval=1min&apikey=JWUNO7YKNBXQFLAW";
                using (var response = await httpClient.GetAsync(apiUri))
                {
                    string apiResponse = await response.Content.ReadAsStringAsync();
                    var data = (JObject)JsonConvert.DeserializeObject<object>(apiResponse);
                    var metadata = data["Meta Data"];
                    Console.WriteLine(metadata);
                    var lastRefreshed = metadata["3. Last Refreshed"].Value<DateTime>();
                    var startData = lastRefreshed.AddMinutes(-30);
                    var dataTimeSeries = data["Time Series (1min)"];
                    var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

                    Action<DeliveryReport<Null, string>> handler = r =>
                        Console.WriteLine(!r.Error.IsError
                        ? $"Delivered message to {r.TopicPartitionOffset}"
                        : $"Delivery Error: {r.Error.Reason}");
                    using (var producer = new ProducerBuilder<Null, string>(config).Build())
                    {
                        await DeleteTopics(config.BootstrapServers, new string[] { "tp1-topic" });
                        while (startData < lastRefreshed)
                        {
                            var key = startData.ToString("yyyy-MM-dd HH:mm:ss");
                            if (dataTimeSeries[key] != null)
                            {

                                var jsonPayload = JsonConvert.SerializeObject(dataTimeSeries[key].Value<object>());
                                Console.WriteLine("send to kafka: ");
                                Console.WriteLine(jsonPayload);
                                await producer.ProduceAsync("tp1-topic", new Message<Null, string> { Value = jsonPayload });
                                producer.Flush(TimeSpan.FromSeconds(10));


                                await Task.Delay(5000);
                            }
                            startData = startData.AddMinutes(1);
                        }
                    }

                }
            }
        }
        static void Main(string[] args)
        {
            GetStockResponse().Wait();

            Console.ReadLine();
        }
    }
}
