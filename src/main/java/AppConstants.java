public final class AppConstants {

    public static final String QUEUE_NAME = "hello";

    public interface RabbitMqErrorCode {
        String NEW_CONNECTION = "59999";
        String CREATE_CHANNEL = "59998";
        String QUEUE_DECLARATION = "59997";
        String CHANNEL_PUBLISH = "59996";
        String CHANNEL_CONSUME = "59995";
    }
}
