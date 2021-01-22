package programs;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.logging.Level;

/**
 * https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/Send.java
 */
public final class RabbitMqSender {

    private Channel channel;

    public void init() {
        final ConnectionFactory factory = new RabbitMqHelper.ConnectionFactoryBuilder()
                .setHostName("127.0.0.1")
                .build();
        final RabbitMqHelper rabbitMqHelper = new RabbitMqHelper();

        final IeApiResponse<Connection> connectionResponse = rabbitMqHelper.newConnection(factory);
        if (null != connectionResponse.error) { return; }
        final Connection connection = connectionResponse.result;
        if (null == connection) {
            java.util.logging.Logger.getLogger("programs.RabbitMqReceiver").log(Level.SEVERE, "init - connection is null!!");
            return;
        }

        final IeApiResponse<Channel> channelResponse = rabbitMqHelper.createChannel(connection);
        if (null != channelResponse.error) { return; }
        final Channel channel = channelResponse.result;
        if (null == channel) {
            java.util.logging.Logger.getLogger("programs.RabbitMqReceiver").log(Level.SEVERE, "init - channel is null!!");
            return;
        }
        this.channel = channel;

        final RabbitMqHelper.QueueParameter parameter = new RabbitMqHelper.QueueParameter(AppConstants.QUEUE_NAME);
        final IeApiResponse<Boolean> queueDeclarationResponse = rabbitMqHelper.queueDeclare(this.channel, parameter);
        if (null != queueDeclarationResponse.error) { return; }

        final IeApiResponse<Boolean> publishResponse = rabbitMqHelper.basicPublish(
                channel, "", AppConstants.QUEUE_NAME, "aA1234567890", null);
    }
}
