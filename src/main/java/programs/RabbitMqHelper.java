package programs;

import com.rabbitmq.client.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.logging.Level;

/**
 * [Java Client API Guide](https://www.rabbitmq.com/api-guide.html)
 */
public final class RabbitMqHelper {

    @NotNull
    public IeApiResponse<Connection> newConnection(@NotNull final ConnectionFactory factory) {
        try {
            final Connection connection = factory.newConnection();
            return new IeApiResponse<>(connection, null);
        }
        catch (Exception cause) {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.SEVERE, "Error on ConnectionFactory#newConnection(): " + cause.getLocalizedMessage());
            final IeRuntimeException error = new IeRuntimeException(cause, AppConstants.RabbitMqErrorCode.NEW_CONNECTION);
            return new IeApiResponse<>(null, error);
        }
    }

    @NotNull
    public IeApiResponse<Channel> createChannel(@NotNull final Connection connection) {
        try {
            final Channel channel = connection.createChannel();
            return new IeApiResponse<>(channel, null);
        }
        catch (Exception cause) {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.SEVERE, "Error on Connection#createChannel(): " + cause.getLocalizedMessage());
            final IeRuntimeException error = new IeRuntimeException(cause, AppConstants.RabbitMqErrorCode.CREATE_CHANNEL);
            return new IeApiResponse<>(null, error);
        }
    }

    @NotNull
    public IeApiResponse<Boolean> queueDeclare(@NotNull final Channel channel, QueueParameter parameter) {
        try {
            channel.queueDeclare(
                    parameter.name, parameter.durable, parameter.exclusive, parameter.autoDelete, parameter.arguments);
            return new IeApiResponse<>(true, null);
        }
        catch (Exception cause) {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.SEVERE, "Error on Channel#queueDeclare(): " + cause.getLocalizedMessage());
            final IeRuntimeException error = new IeRuntimeException(cause, AppConstants.RabbitMqErrorCode.QUEUE_DECLARATION);
            return new IeApiResponse<>(null, error);
        }
    }

    @NotNull
    public IeApiResponse<Boolean> basicPublish(
            @NotNull final Channel channel,
            @NotNull final String exchange,
            @NotNull final String queueName,
            @NotNull final String message,
            @Nullable final AMQP.BasicProperties properties) {
        try {
            channel.basicPublish(exchange, queueName, properties, message.getBytes(StandardCharsets.UTF_8));
            return new IeApiResponse<>(true, null);
        }
        catch (Exception cause) {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.SEVERE, "Error on Channel#basicPublish(): " + cause.getLocalizedMessage());
            final IeRuntimeException error = new IeRuntimeException(cause, AppConstants.RabbitMqErrorCode.CHANNEL_PUBLISH);
            return new IeApiResponse<>(null, error);
        }
    }

    @NotNull
    public IeApiResponse<Boolean> basicConsume(
            @NotNull final Channel channel,
            @NotNull final String queueName,
            @NotNull final DeliverCallback deliverCallback) {
        return basicConsume(channel, queueName, deliverCallback, new DefaultCancelCallback(), true);
    }

    @NotNull
    public IeApiResponse<Boolean> basicConsume(
            @NotNull final Channel channel,
            @NotNull final String queueName,
            @NotNull final DeliverCallback deliverCallback,
            @NotNull final CancelCallback cancelCallback) {
        return basicConsume(channel, queueName, deliverCallback, cancelCallback, true);
    }

    @NotNull
    public IeApiResponse<Boolean> basicConsume(
            @NotNull final Channel channel,
            @NotNull final String queueName,
            @NotNull final DeliverCallback deliverCallback,
            @NotNull final CancelCallback cancelCallback,
            final boolean autoAckFlag) {
        try {
            channel.basicConsume(queueName, autoAckFlag, deliverCallback, cancelCallback);
            return new IeApiResponse<>(true, null);
        }
        catch (Exception cause) {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.SEVERE, "Error on Channel#basicConsume(): " + cause.getLocalizedMessage());
            final IeRuntimeException error = new IeRuntimeException(cause, AppConstants.RabbitMqErrorCode.CHANNEL_CONSUME);
            return new IeApiResponse<>(null, error);
        }
    }

    public static final class QueueParameter {
        @Nullable
        public final String name;
        public final boolean durable;
        public final boolean exclusive;
        public final boolean autoDelete;
        @Nullable
        public final Map<String, Object> arguments;

        public QueueParameter(@Nullable final String name) {
            this.name = name;
            this.durable = false;
            this.exclusive = false;
            this.autoDelete = false;
            this.arguments = null;
        }

        public QueueParameter(
                @Nullable final String name,
                final boolean durable,
                final boolean exclusive,
                final boolean autoDelete,
                @Nullable final Map<String, Object> arguments) {
            this.name = name;
            this.durable = durable;
            this.exclusive = exclusive;
            this.autoDelete = autoDelete;
            this.arguments = arguments;
        }
    }

    public static final class ConnectionFactoryBuilder {
        private String userName = null;
        private String password = null;
        private String virtualHost = "/";
        private String hostName = "localhost";
        private int portNumber = 5672;

        @NotNull
        public ConnectionFactoryBuilder setUserName(@Nullable final String userName) {
            if ( (null != userName) && (userName.length() > 0) ){
                this.userName = userName;
            }
            return this;
        }

        @NotNull
        public ConnectionFactoryBuilder setPassword(@Nullable final String password) {
            if ( (null != password) && (password.length() > 0) ){
                this.password = password;
            }
            return this;
        }

        @NotNull
        public ConnectionFactoryBuilder setVirtualHost(@Nullable final String virtualHost) {
            if ( (null != virtualHost) && (virtualHost.length() > 0) ){
                this.virtualHost = virtualHost;
            }
            return this;
        }

        @NotNull
        public ConnectionFactoryBuilder setHostName(@Nullable final String hostName) {
            if ( (null != hostName) && (hostName.length() > 0) ){
                this.hostName = hostName;
            }
            return this;
        }

        @NotNull
        public ConnectionFactoryBuilder setPortNumber(final int portNumber) {
            if (portNumber > 0) { this.portNumber = portNumber; }
            return this;
        }

        @NotNull
        public ConnectionFactory build() {
            final ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            if ( (null != userName) && (null != password) ) {
                java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.INFO,
                        "ConnectionFactoryBuilder - userName: [" + userName + "], password: [" + password + "]");
                factory.setUsername(userName);
                factory.setPassword(password);
            }
            if (null != virtualHost) {
                java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.INFO,
                        "ConnectionFactoryBuilder - virtualHost: [" + virtualHost + "]");
                factory.setVirtualHost(virtualHost);
            }
            if (null != hostName) {
                java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.INFO,
                        "ConnectionFactoryBuilder - hostName: [" + hostName + "]");
                factory.setHost(hostName);
            }

            if (portNumber > 0) {
                java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.INFO,
                        "ConnectionFactoryBuilder - portNumber: [" + portNumber + "]");
                factory.setPort(portNumber);
            }
            return factory;
        }
    }

    public static final class DefaultCancelCallback implements CancelCallback {
        @Override
        public void handle(String consumerTag) throws IOException {
            java.util.logging.Logger.getLogger("programs.RabbitMqHelper").log(Level.INFO,
                    "DefaultCancelCallback - consumerTag: [" + consumerTag + "]");
        }
    }
}
