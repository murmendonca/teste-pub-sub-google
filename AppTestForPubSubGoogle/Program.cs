// See https://aka.ms/new-console-template for more information
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using System.Text;
using Grpc.Core;
using System.Text.Json;

public class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("Hello, World!");

        const string ProjectId = "shared-architecture-dev";
        //const string ProjectId = "shared-architecture-hml";
        const string TopicId = "digital-nps-dev-patient-data-received";
        //const string TopicId = "digital-nps-hml-patient-data-received";
        const string SubscriptionId = "digital-nps-dev-patient-data-received-processing";
        //const string SubscriptionId = "digital-nps-hml-patient-data-received-processing";

        // 1. Inicia o processo de publicação e criação de recursos
        await PubAsync($"Iniciando a publicação, data atual {DateTime.Now}");

        // 2. Inicia o processo de consumo (recebimento)
        //await ReceiveMessage();


        async Task PubAsync(string message)
        {
            var topicName = new TopicName(ProjectId, TopicId);
            var subName = new SubscriptionName(ProjectId, SubscriptionId);

            var publisherService = await new PublisherServiceApiClientBuilder
            {
                Endpoint = "localhost:8085",
                ChannelCredentials = ChannelCredentials.Insecure
            }.BuildAsync();

            // 1. Criar Tópico
            try
            {
                Console.WriteLine($"Tópico {TopicId} criado.");
            }
            catch (RpcException ex) when (ex.Status.StatusCode == StatusCode.AlreadyExists)
            {
                Console.WriteLine($"Tópico {TopicId} já existe.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao criar Tópico: {ex.Message}");
            }

            var subscriberService = await new SubscriberServiceApiClientBuilder
            {
                Endpoint = "localhost:8085",
                ChannelCredentials = ChannelCredentials.Insecure
            }.BuildAsync();

            // 3. Publicar Mensagem
            Console.WriteLine("\nPublicando mensagem...");

            var publisher = await PublisherClient.CreateAsync(topicName);
            var encounterCodeRandom = new Random(1000).Next();

            try
            {
                message = JsonSerializer.Serialize(new
                {
                    ds_unidade = "Hospital Bahia",
                    email = $"francisco.murilo.ext@americasmed.com.br",
                    nm_paciente = "Murilo Mendonça",
                    nr_celular = "96140978",
                    nr_cpf = "437.856.780-61",
                    cd_atendimento = "06",
                    nm_prestador = "FERNANDO VELASCO FRIEDRICH",
                    ds_especialidade = "CIRURGIA GERAL",
                    tipo_porta = "Pronto Socorro",
                    dt_atendimento = "2026-01-05 00:00:0000:00",
                    dt_alta = "2026-01-04 00:00:0000:00",
                    sn_primeiro_atendimento = "N",
                    sn_onco = "N",
                    quarentena = "FALSE"

                });

                var pubsubMessage = new PubsubMessage
                {
                    Data = ByteString.CopyFromUtf8(message),
                    Attributes = { { "source", "csharp-emulator-test" } }
                };

                string messageId = await publisher.PublishAsync(pubsubMessage);

                Console.WriteLine($"Mensagem publicada com ID: {messageId}");
            }
            finally
            {
                await publisher.ShutdownAsync(TimeSpan.FromSeconds(5));
            }
        }

        async Task ReceiveMessage()
        {
            var subName = new SubscriptionName(ProjectId, SubscriptionId);

            Console.WriteLine("\nAguardando 1 mensagem...");

            var subscriber = await new SubscriberClientBuilder
            {
                SubscriptionName = subName,
                Endpoint = "localhost:8085",
                ChannelCredentials = ChannelCredentials.Insecure
            }.BuildAsync();

            int messageCount = 0;
            using var cts = new CancellationTokenSource();

            await subscriber.StartAsync(async (msg, token) =>
            {
                string receivedMessage = System.Text.Encoding.UTF8.GetString(msg.Data.ToByteArray());
                string attributes = string.Join(", ", msg.Attributes.Select(kv => $"{kv.Key}={kv.Value}"));

                Console.WriteLine($"Mensagem Recebida (ID: {msg.MessageId}):");
                Console.WriteLine($"  Data: {receivedMessage}");
                Console.WriteLine($"  Atributos: {attributes}");

                messageCount++;
                if (messageCount >= 1)
                {
                    cts.Cancel(); // Sinaliza o fim do consumo
                }

                return SubscriberClient.Reply.Ack;
            });

            // Aguarda o cancelamento (após receber a mensagem) ou o timeout
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
            }
            catch (TaskCanceledException)
            {
                // Cancelamento esperado
            }
            finally
            {
                await subscriber.StopAsync(TimeSpan.FromSeconds(5));
                Console.WriteLine("\nTeste local do Pub/Sub finalizado.");
            }
        }
    }
}