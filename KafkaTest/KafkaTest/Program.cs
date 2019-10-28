using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaProducer
{
    public class StockPayLoadData
    {
        public DateTime Time { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Close { get; set; }
        public double Volume { get; set; }
        public double Low { get; set; }

    }
    public class Weather
    {
        public Weather()
        {
            Areas = new List<Area>();
        }
        public string Date { get; set; }
        public List<Area> Areas { get; set; }
    }
    public class Area
    {
        public string City { get; set; }
        public Area()
        {
            TimeRanges = new List<TimeRange>();
        }
        public List<TimeRange> TimeRanges { get; set; }
    }
    public class TimeRange
    {
        public string Time { get; set; }
        public string Value { get; set; }
    }

    class Program
    {
        private static async Task UpdateWeather()
        {
            XmlTextReader reader = new XmlTextReader("http://data.bmkg.go.id/datamkg/MEWS/DigitalForecast/DigitalForecast-JawaBarat.xml");
            XmlDocument doc = new XmlDocument();
            doc.Load(reader);
            var m = JsonConvert.SerializeXmlNode(doc);
            var data = (JObject)JsonConvert.DeserializeObject<object>(m);
            var forecast = data["data"].Value<JObject>("forecast");
            var issue = forecast["issue"].Value<string>("timestamp");
            var areas = forecast["area"];
            var weather = new Weather();
            foreach (var area in areas)
            {
                var name = area["name"][0].Value<string>("#text");
                var areaObj = new Area
                {
                    City = name
                };
                if (area["parameter"] != null)
                {
                    var parameter = area["parameter"][5];
                    var description = parameter.Value<string>("Description");
                    var timeRanges = parameter["timerange"];
                    foreach (var time in timeRanges)
                    {
                        var datetime = time["@datetime"].Value<string>();
                        var value = time["value"][0].Value<string>("#text");
                        var timeRange = new TimeRange();
                        timeRange.Time = datetime;
                        timeRange.Value = value;
                        areaObj.TimeRanges.Add(timeRange);

                    }
                }

                weather.Areas.Add(areaObj);

            }
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            Action<DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                await DeleteTopics(config.BootstrapServers, new string[] { "weather-topic" });

                foreach (var area in weather.Areas)
                {
                    var jsonPayload = JsonConvert.SerializeObject(area);
                    Console.WriteLine("send to kafka: ");
                    Console.WriteLine(jsonPayload);
                    await producer.ProduceAsync("weather-topic", new Message<Null, string> { Value = jsonPayload });
                    producer.Flush(TimeSpan.FromSeconds(10));
                    await Task.Delay(5000);
                }


            }

        }

        private static async Task DeleteTopics(string brokerList, string[] topicNames)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerList }).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(topicNames, null);
                }
                catch
                {
                }
            }
        }
        private static async Task SendToKafka(string topicId)
        {
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
                        await DeleteTopics(config.BootstrapServers, new string[] { "mib-topic" });
                        while (startData < lastRefreshed)
                        {
                            var key = startData.ToString("yyyy-MM-dd HH:mm:ss");
                            if (dataTimeSeries[key] != null)
                            {
                                var payload = new StockPayLoadData
                                {
                                    Time = startData,
                                    Open = dataTimeSeries[key].Value<double>("1. open"),
                                    High = dataTimeSeries[key].Value<double>("2. high"),
                                    Low = dataTimeSeries[key].Value<double>("3. low"),
                                    Close = dataTimeSeries[key].Value<double>("4. close"),
                                    Volume = dataTimeSeries[key].Value<double>("5. volume")

                                };
                                var jsonPayload = JsonConvert.SerializeObject(payload);
                                Console.WriteLine("send to kafka: ");
                                Console.WriteLine(jsonPayload);
                                await producer.ProduceAsync("mib-topic", new Message<Null, string> { Value = jsonPayload });
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
            //GetStockResponse().Wait();
            UpdateWeather().Wait();
            Console.ReadLine();
        }
    }
}
