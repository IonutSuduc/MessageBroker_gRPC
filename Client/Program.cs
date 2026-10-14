using Broker; // Folosește "Broker" cu literă mare
using Common;
using Grpc.Net.Client;
using System.Text;

namespace Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.InputEncoding = Encoding.UTF8;

            // Conectare prin gRPC channel la adresa brokerului
            // 1. Configurăm un handler care permite utilizarea certificatului local auto-semnat în mediu de dezvoltare
            var handler = new HttpClientHandler();
            handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;

            // 2. Schimbăm schema în "https" și folosim portul configurat (ex: 5001)
            string address = $"https://{Settings.ServerIp}:5001";

            // 3. Creăm canalul gRPC trecând handler-ul de mai sus
            using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions { HttpHandler = handler });
            var client = new MessageBrokerService.MessageBrokerServiceClient(channel);

            string[] topics = Settings.Topics;

            Console.WriteLine("========================================");
            Console.WriteLine("   SISTEM gRPC - CLIENT INTERACTIV     ");
            Console.WriteLine("========================================");
            Console.WriteLine("1. Vreau să trimit mesaje (Sender)");
            Console.WriteLine("2. Vreau să ascult mesaje (Receiver)");
            Console.Write("Alege opțiunea (1 sau 2): ");

            string roleChoice = Console.ReadLine();

            Console.WriteLine("\nAlege subiectul dorit:");
            for (int i = 0; i < topics.Length; i++)
            {
                Console.WriteLine($"{i + 1}. {topics[i]}");
            }
            Console.Write("Introdu numărul subiectului: ");

            if (int.TryParse(Console.ReadLine(), out int topicIndex) && topicIndex >= 1 && topicIndex <= topics.Length)
            {
                // Corectat de la topicIndex1 la topicIndex - 1
                string selectedTopic = topics[topicIndex - 1];

                if (roleChoice == "1")
                {
                    Console.Title = $"gRPC SENDER - [{selectedTopic}]";
                    await RunAsSenderAsync(client, selectedTopic);
                }
                else if (roleChoice == "2")
                {
                    Console.Title = $"gRPC RECEIVER - [{selectedTopic}]";
                    Console.Write("Dorești să încarci și mesajele precedente (istoricul)? (da/nu): ");
                    string historyChoice = Console.ReadLine()?.Trim().ToLower();
                    bool loadHistory = (historyChoice == "da" || historyChoice == "d");

                    await RunAsReceiverAsync(client, selectedTopic, loadHistory);
                }
            }
            else
            {
                Console.WriteLine("Opțiune invalidă!");
            }

            Console.WriteLine("\nApasă orice tastă pentru a ieși...");
            Console.ReadKey();
        }

        private static async Task RunAsSenderAsync(MessageBrokerService.MessageBrokerServiceClient client, string subject)
        {
            Console.Write("Introdu ID-ul tău (ex: Sender_Ana): ");
            string senderId = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(senderId)) senderId = "Anonymous";

            Console.WriteLine($"\n[SENDER gRPC ACTIV] Scrie mesajele pentru '{subject}'. Tastează 'exit' pentru oprire.\n");

            while (true)
            {
                Console.Write("Mesaj: ");
                string content = Console.ReadLine();
                if (content?.ToLower() == "exit") break;
                if (string.IsNullOrWhiteSpace(content)) continue;

                var request = new MessageRequest
                {
                    Subject = subject,
                    SenderId = senderId,
                    Content = content
                };

                try
                {
                    var response = await client.PublishMessageAsync(request);
                    Console.WriteLine($"[RĂSPUNS BROKER]: {response.Message}\n");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[EROARE gRPC]: {ex.Message}");
                }
            }
        }

        private static async Task RunAsReceiverAsync(MessageBrokerService.MessageBrokerServiceClient client, string subject, bool loadHistory)
        {
            try
            {
                var request = new SubscribeRequest
                {
                    Subject = subject,
                    LoadHistory = loadHistory
                };

                using var call = client.Subscribe(request);
                Console.WriteLine($"\n[RECEIVER gRPC] Conectat cu succes la fluxul pentru '{subject}'...\n");

                while (await call.ResponseStream.MoveNext(CancellationToken.None))
                {
                    var currentMessage = call.ResponseStream.Current;
                    Console.WriteLine($"[{currentMessage.SenderId}]: {currentMessage.Content} ({currentMessage.Timestamp})");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EROARE RECEIVER gRPC]: {ex.Message}");
            }
        }
    }
}