import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;


/**
 * https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/Recv.java
 */
public final class RabbitMqReceiver implements DeliverCallback {

    public void init() {
        final ConnectionFactory factory = new RabbitMqHelper.ConnectionFactoryBuilder()
                .setHostName("127.0.0.1")
                .build();
        final RabbitMqHelper rabbitMqHelper = new RabbitMqHelper();

        final IeApiResponse<Connection> connectionResponse = rabbitMqHelper.newConnection(factory);
        if (null != connectionResponse.error) { return; }
        final Connection connection = connectionResponse.result;
        if (null == connection) {
            java.util.logging.Logger.getLogger("RabbitMqReceiver").log(Level.SEVERE, "init - connection is null!!");
            return;
        }

        final IeApiResponse<Channel> channelResponse = rabbitMqHelper.createChannel(connection);
        if (null != channelResponse.error) { return; }
        final Channel channel = channelResponse.result;
        if (null == channel) {
            java.util.logging.Logger.getLogger("RabbitMqReceiver").log(Level.SEVERE, "init - channel is null!!");
            return;
        }

        final RabbitMqHelper.QueueParameter parameter = new RabbitMqHelper.QueueParameter(AppConstants.QUEUE_NAME);
        final IeApiResponse<Boolean> queueDeclarationResponse = rabbitMqHelper.queueDeclare(channel, parameter);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    @Override
    public void handle(String consumerTag, Delivery delivery) throws IOException {
        final String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        System.out.println(" [x] Received '" + message + "'");
        java.util.logging.Logger.getLogger("RabbitMqReceiver").log(Level.INFO, "handle: [x] Received '" + message + "'");
    }
}
