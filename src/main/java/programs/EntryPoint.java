package programs;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe;
import kotlin.text.Charsets;
import org.jetbrains.annotations.NotNull;
import programs.delegates.IeApiResult;
import programs.models.IePair;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;


public final class EntryPoint {
    public static void main(String[] args) {
        //new RabbitMqReceiver().init();

        final HiveMqttHelper.ClientConfig clientConfig = new HiveMqttHelper.ClientConfig.Builder()
                .setIdentifier(UUID.randomUUID().toString())
                .build();
        java.util.logging.Logger.getLogger("EntryPoint").log(Level.INFO, "main - clientConfig");
        final Mqtt3Subscribe subscribeMessage = Mqtt3Subscribe.builder()
                        .topicFilter("test/topic")
                        .qos(MqttQos.EXACTLY_ONCE)
                        .build();
        java.util.logging.Logger.getLogger("EntryPoint").log(Level.INFO, "main - subscribeMessage ");
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final HiveMqttHelper.Mqtt3ClientSubscribeTask subscribeTask = new HiveMqttHelper.Mqtt3ClientSubscribeTask(clientConfig, subscribeMessage, executorService);
        subscribeTask.callback = new Mqtt3ClientSubscribeCallback();
        subscribeTask.runTask();
        while (true) {

        }
    }

    private static class Mqtt3ClientSubscribeCallback implements IeApiResult<IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>> {

        @Override
        public void onSuccess(@NotNull final IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>> response) {
            java.util.logging.Logger.getLogger("EntryPoint").log(Level.INFO, "Mqtt3ClientSubscribeCallback#onSuccess");
            if (response.result == null) {
                java.util.logging.Logger.getLogger("EntryPoint").log(Level.SEVERE, "Mqtt3ClientSubscribeCallback#onSuccess - response.result == null");
                return;
            }

            final Mqtt3Client mqtt3Client = response.result.first;
            final LongRunningTask longRunningTask = new LongRunningTask();
            longRunningTask.setTask(new Mqtt3ConsumptionTask(mqtt3Client));
            longRunningTask.startTask();
        }

        @Override
        public void onError(@NotNull final IeRuntimeException cause) {
            java.util.logging.Logger.getLogger("EntryPoint").log(Level.SEVERE, "Mqtt3ClientSubscribeCallback#onError", cause.fillInStackTrace());
        }
    }

    private static class Mqtt3ConsumptionTask implements Runnable {

        final Mqtt3Client mqtt3Client;

        Mqtt3ConsumptionTask (@NotNull final Mqtt3Client mqtt3Client) {
            this.mqtt3Client = mqtt3Client;
        }

        @Override
        public void run() {
            try  {
                final Mqtt3BlockingClient.Mqtt3Publishes publishes = mqtt3Client.toBlocking().publishes(MqttGlobalPublishFilter.ALL);
                final Mqtt3Publish publishMessage = publishes.receive();
                final String message = new String(publishMessage.getPayloadAsBytes(), Charsets.UTF_8);
                java.util.logging.Logger.getLogger("EntryPoint").log(Level.INFO, "Mqtt3ConsumptionTask: " + message);
                // or with timeout
                //final Optional<Mqtt3Publish> publishMessage = publishes.receive(10, TimeUnit.SECONDS);
                // or without blocking
                //final Optional<Mqtt3Publish> publishMessage = publishes.receiveNow();
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("EntryPoint").log(Level.SEVERE, "Error on Mqtt3ConsumptionTask", cause.fillInStackTrace());
            }
        }
    }
}
