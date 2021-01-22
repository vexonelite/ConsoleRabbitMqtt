package programs;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import programs.rxjava2.AbstractRxTask;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;


public class HiveMqttHelper {

    public static final class ClientConfig {
        public final String identifier;
        public final String hostName;
        public final int portNumber;

        private ClientConfig(
                @NotNull final String identifier,
                @NotNull final String hostName,
                final int portNumber) {
            this.identifier = identifier;
            this.hostName = hostName;
            this.portNumber = portNumber;
        }

        public static final class Builder {
            private String identifier = null;
            private String hostName = "localhost";
            private int portNumber = 1883;

            @NotNull
            public Builder setIdentifier(@Nullable final String identifier) {
                if ( (null != identifier) && (identifier.length() > 0) ){
                    this.identifier = identifier;
                }
                return this;
            }

            @NotNull
            public Builder setHostName(@Nullable final String hostName) {
                if ( (null != hostName) && (hostName.length() > 0) ){
                    this.hostName = hostName;
                }
                return this;
            }

            @NotNull
            public Builder setPortNumber(final int portNumber) {
                if (portNumber > 0) { this.portNumber = portNumber; }
                return this;
            }

            @NotNull
            public ClientConfig build() {
                if (null == identifier) {
                    identifier = UUID.randomUUID().toString() + "_" + System.currentTimeMillis();
                }
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "ClientConfigBuilder - identifier: [" + identifier + "]");

                if (null != hostName) {
                    java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "ClientConfigBuilder - hostName: [" + hostName + "]");
                }
                if (portNumber > 0) {
                    java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "ClientConfigBuilder - portNumber: [" + portNumber + "]");
                }
                return new ClientConfig(identifier, hostName, portNumber);
            }
        }
    }


    @NotNull
    public Mqtt3AsyncClient buildMqtt3AsyncClient(@NotNull final ClientConfig clientConfig) {
        Mqtt3Client client = MqttClient.builder().useMqttVersion3().build();
        return MqttClient.builder()
                .useMqttVersion3()
                .identifier(clientConfig.identifier)
                .serverHost(clientConfig.hostName)
                .serverPort(clientConfig.portNumber)
                .sslWithDefaultConfig()
                .buildAsync();
    }

    ///
    
    public static final class BuildMqtt3ClientTask
            extends AbstractRxTask<Mqtt3Client> implements Callable<Mqtt3Client> {

        final ClientConfig clientConfig;
        final ExecutorService executorService;

        public BuildMqtt3ClientTask(
                @NotNull final ClientConfig clientConfig,
                @NotNull final ExecutorService executorService) {
            this.clientConfig = clientConfig;
            this.executorService = executorService;
        }

        @Override
        public void runTask() {
            rxDisposeIfPossible();
            setDisposable(
                    Single.fromCallable(this)
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.from(executorService))
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }

        @Override
        public Mqtt3Client call() throws Exception {
            try {
                return MqttClient.builder()
                        .useMqttVersion3()
                        .identifier(clientConfig.identifier)
                        .serverHost(clientConfig.hostName)
                        .serverPort(clientConfig.portNumber)
                        .sslWithDefaultConfig()
                        .build();
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "error on BuildMqtt3ClientTask: " + cause.getLocalizedMessage());
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.BUILD_CLIENT);
            }
        }
    }
}
