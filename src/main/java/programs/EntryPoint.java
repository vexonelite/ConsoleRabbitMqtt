package programs;

public final class EntryPoint {
    public static void main(String[] args) {
        new RabbitMqReceiver().init();
    }
}
