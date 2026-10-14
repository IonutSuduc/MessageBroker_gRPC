using Broker;
using Common;
using Grpc.Core;
using System.Collections.Concurrent;

namespace MessageBroker_Lab2.Services
{
    public class BrokerService : MessageBrokerService.MessageBrokerServiceBase
    {
        // Stocarea în memorie folosind colecții Thread-Safe
        private static ConcurrentDictionary<string, ConcurrentQueue<MessageResponse>> messageStorage =
            new ConcurrentDictionary<string, ConcurrentQueue<MessageResponse>>();

        private static ConcurrentDictionary<string, List<IServerStreamWriter<MessageResponse>>> subscribers =
            new ConcurrentDictionary<string, List<IServerStreamWriter<MessageResponse>>>();

        static BrokerService()
        {
            foreach (var topic in Settings.Topics)
            {
                messageStorage[topic] = new ConcurrentQueue<MessageResponse>();
                subscribers[topic] = new List<IServerStreamWriter<MessageResponse>>();
            }
        }

        public override Task<PublishResponse> PublishMessage(MessageRequest request, ServerCallContext context)
        {
            try
            {
                // Validarea datelor primite
                if (string.IsNullOrWhiteSpace(request.Subject) ||
                    string.IsNullOrWhiteSpace(request.Content) ||
                    !messageStorage.ContainsKey(request.Subject))
                {
                    return Task.FromResult(new PublishResponse { Success = false, Message = "ERROR: Mesaj invalid sau subiect inexistent." });
                }

                var msgResponse = new MessageResponse
                {
                    Subject = request.Subject,
                    SenderId = string.IsNullOrEmpty(request.SenderId) ? "Anonymous" : request.SenderId,
                    Content = request.Content,
                    Timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                };

                // Salvăm în storage-ul transient
                messageStorage[request.Subject].Enqueue(msgResponse);
                Console.WriteLine($"[STOCARE gRPC] Mesaj nou pentru '{request.Subject}' de la '{request.SenderId}'.");

                // Transmitem asincron prin streaming către subscriberi
                _ = BroadcastToSubscribersAsync(msgResponse);

                return Task.FromResult(new PublishResponse { Success = true, Message = "ACK: Mesaj stocat și distribuit." });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new PublishResponse { Success = false, Message = $"ERROR: {ex.Message}" });
            }
        }

        public override async Task Subscribe(SubscribeRequest request, IServerStreamWriter<MessageResponse> responseStream, ServerCallContext context)
        {
            string topic = request.Subject;

            if (!subscribers.ContainsKey(topic))
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Subiectul nu există."));
            }

            // Înregistrăm subscriberul
            lock (subscribers[topic])
            {
                subscribers[topic].Add(responseStream);
            }
            Console.WriteLine($"[gRPC SUBSCRIBE] Un client s-a abonat la '{topic}' (Istoric: {request.LoadHistory})");

            try
            {
                // Dacă s-a cerut istoric, trimitem mesajele din RAM
                if (request.LoadHistory && messageStorage.TryGetValue(topic, out var queue))
                {
                    foreach (var pastMsg in queue)
                    {
                        await responseStream.WriteAsync(pastMsg);
                    }
                }

                // Menținem conexiunea de streaming deschisă atât timp cât clientul este conectat
                while (!context.CancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(1000);
                }
            }
            catch
            {
                // Client deconectat
            }
            finally
            {
                lock (subscribers[topic])
                {
                    subscribers[topic].Remove(responseStream);
                }
                Console.WriteLine($"[gRPC DECONECTARE] Un client s-a deconectat de la '{topic}'.");
            }
        }

        private async Task BroadcastToSubscribersAsync(MessageResponse message)
        {
            if (subscribers.TryGetValue(message.Subject, out var list))
            {
                List<IServerStreamWriter<MessageResponse>> deadList = new List<IServerStreamWriter<MessageResponse>>();
                lock (list)
                {
                    foreach (var writer in list)
                    {
                        try
                        {
                            writer.WriteAsync(message).Wait();
                        }
                        catch
                        {
                            deadList.Add(writer);
                        }
                    }

                    foreach (var dead in deadList)
                    {
                        list.Remove(dead);
                    }
                }
            }
        }
    }
}