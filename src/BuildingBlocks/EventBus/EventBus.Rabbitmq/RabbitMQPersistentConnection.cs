using System;
using System.Net.Sockets;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.Rabbitmq
{
    public class RabbitMQPersistentConnection : IDisposable
    {
        private readonly IConnectionFactory connectionFactory;
        private readonly int _retryCount;
        private IConnection connection;
        private object lock_object = new object();
        private bool _disposed;
        
        public RabbitMQPersistentConnection(IConnectionFactory connectionFactory, int retryCount = 5)
        {
            this.connectionFactory = connectionFactory;
            _retryCount = retryCount;
        }
        
        public bool IsConnection => connection != null && connection.IsOpen;

        public IModel CreateModel()
        {
            return connection.CreateModel();
        }
        
        public void Dispose()
        {
            _disposed = true;
            connection.Dispose();
        }

        public bool TryConnection()
        {
            lock (lock_object)
            {
                var policy = Policy.Handle<SocketException>()
                    .Or<BrokerUnreachableException>()
                    .WaitAndRetry(_retryCount, retryAttemp => TimeSpan.FromSeconds(Math.Pow(2, retryAttemp)),
                        (ex, time) =>
                        {  
                        }
                    );

                policy.Execute(() =>
                {
                    connection = connectionFactory.CreateConnection();
                });

                if (IsConnection)
                {
                    connection.ConnectionShutdown += Connection_ConnectionShutDown;
                    connection.CallbackException += Connection_CallbackException;
                    connection.ConnectionBlocked += Connection_ConnectionBlocked;
                    //log
                    return true;
                }
                return false;
            }
        }

        private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
        {
            if(!_disposed) return;
            TryConnection();
        }

        private void Connection_CallbackException(object? sender, CallbackExceptionEventArgs e)
        {
            if (!_disposed) return;
            TryConnection();
        }

        private void Connection_ConnectionShutDown(object sender, ShutdownEventArgs e)
        {
            //log Connection_ConnectionShutdown
            if (!_disposed) return;
            TryConnection();
        }
    }
}