using System;
using System.Threading;
using Confluent.Kafka;

public class Consumer
{
    static void Main(string[] args)
    {
        string command = args[0];
        var config = new ConsumerConfig
        {
            // User-specific properties that you must set
            BootstrapServers = "localhost:29092",

            // Fixed properties
            GroupId = $"kafka-dotnet-getting-started-{command}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        const string topic = "purchases";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);
        try
        {
            while (true)
            {
                ConsumeResult<string, string> cr = consumer.Consume(cts.Token);
                Console.WriteLine(
                    $"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C was pressed.
        }
        finally
        {
            consumer.Close();
        }
    }
}