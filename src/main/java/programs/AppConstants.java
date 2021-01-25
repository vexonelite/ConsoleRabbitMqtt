package programs;

public final class AppConstants {

    public static final String QUEUE_NAME = "hello";

    public interface RabbitMqErrorCode {
        String NEW_CONNECTION = "59999";
        String CREATE_CHANNEL = "59998";
        String QUEUE_DECLARATION = "59997";
        String CHANNEL_PUBLISH = "59996";
        String CHANNEL_CONSUME = "59995";
    }

    public interface HiveMqErrorCode {
        String BUILD_CLIENT = "49999";
        String CONNECTION_FAILURE = "49998";
        String DISCONNECTION_FAILURE = "49997";
//                ConnectionFailedException//	if an error occurs before the Connect message could be sent
//                ConnectionClosedException//	if the connection is closed after the Connect message has been sent but before a ConnAck message has been received
//                Mqtt3ConnAckException	//if the ConnAck message contained an error code (the ConnAck message is contained in the exception)
//                MqttClientStateException//	 if the client is already connecting or connected
        String PUBLISH_FAILURE = "59998";
        String SUBSCRIBE_FAILURE = "59999";
    }
}
