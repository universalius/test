using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace WorkerRoleWithSBQueue1
{
    public class WorkerRole : RoleEntryPoint
    {
        // Имя вашей очереди
        const string QueueName = "ProcessingQueue";

        // QueueClient является потокобезопасным. Рекомендуется его кэшировать, 
        // а не создавать заново при каждом запросе
        QueueClient Client;
        ManualResetEvent CompletedEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.WriteLine("Начало обработки сообщений");

            // Запускает конвейер сообщений, и для каждого полученного сообщения осуществляется обратный вызов. Вызов закрытия клиента приведет к остановке конвейера.
            Client.OnMessage((receivedMessage) =>
                {
                    try
                    {
                        // Обработка сообщения
                        Trace.WriteLine("Обработка сообщения Service Bus: " + receivedMessage.SequenceNumber.ToString());
                    }
                    catch
                    {
                        // Здесь обрабатываются любые исключения, связанные с обработкой сообщений
                    }
                });

            CompletedEvent.WaitOne();
        }

        public override bool OnStart()
        {
            // Установка максимального количества одновременных подключений 
            ServicePointManager.DefaultConnectionLimit = 12;

            // Создание очереди, если она еще не существует
            string connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            if (!namespaceManager.QueueExists(QueueName))
            {
                namespaceManager.CreateQueue(QueueName);
            }

            // Инициализация подключения к очереди Service Bus
            Client = QueueClient.CreateFromConnectionString(connectionString, QueueName);
            return base.OnStart();
        }

        public override void OnStop()
        {
            // Закрытие подключения к очереди Service Bus
            Client.Close();
            CompletedEvent.Set();
            base.OnStop();
        }
    }
}