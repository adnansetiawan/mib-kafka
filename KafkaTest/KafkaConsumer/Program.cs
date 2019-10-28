using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

using Newtonsoft.Json;

namespace KafkaConsumer
{
    class Program
    {
        public static async Task SendStockToFirebase(String data)
        {

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    var content = new StringContent(data, Encoding.UTF8, "application/json");
                    var responseMessage = await client.PostAsync("https://kontes-64ef5.firebaseio.com/stocks.json", content);
                }
                catch (Exception ex)
                {
                }

            }
        }
        public static async Task SendWeatherToFirebase(String data)
        {

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    var content = new StringContent(data, Encoding.UTF8, "application/json");
                    var responseMessage = await client.PostAsync("https://kontes-64ef5.firebaseio.com/weather.json", content);
                }
                catch (Exception ex)
                {
                }

            }
        }
        static void Main(string[] args)
        {

            ConsumeWeather();

        }

        private static void ConsumeStock()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "mib",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("mib-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            SendStockToFirebase(cr.Value).Wait();

                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
        private static void ConsumeWeather()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "weather",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("weather-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            SendWeatherToFirebase(cr.Value).Wait();

                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
