package programs;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientConfig;
import com.hivemq.client.mqtt.datatypes.MqttClientIdentifier;
import com.hivemq.client.mqtt.lifecycle.MqttClientConnectedContext;
import com.hivemq.client.mqtt.lifecycle.MqttClientConnectedListener;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import programs.models.IePair;
import programs.rxjava2.AbstractRxTask;

import java.util.Optional;
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
        return MqttClient.builder()
                .useMqttVersion3()
                .identifier(clientConfig.identifier)
                .serverHost(clientConfig.hostName)
                .serverPort(clientConfig.portNumber)
                .sslWithDefaultConfig()
                .buildAsync();
    }

    ///

    public static class ConnectionStatusCallback
            implements MqttClientConnectedListener, MqttClientDisconnectedListener {

        @Override
        public void onConnected(@NotNull MqttClientConnectedContext context) {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "ConnectionStatusCallback - onConnected");
            showClientConfig(context.getClientConfig());
        }

        @Override
        public void onDisconnected(@NotNull MqttClientDisconnectedContext context) {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "ConnectionStatusCallback - onDisconnected");
            showClientConfig(context.getClientConfig());
        }

        private void showClientConfig(@NotNull final MqttClientConfig clientConfig) {
            String identifier = "None";
            final Optional<MqttClientIdentifier> clientIdentifierWrapper = clientConfig.getClientIdentifier();
            if (clientIdentifierWrapper.isPresent()) {
                final MqttClientIdentifier clientIdentifier = clientIdentifierWrapper.get();
                identifier = clientIdentifier.toString();
            }
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "ConnectionStatusCallback - showClientConfig ["
                    + "Id: " + identifier + ", Host: " + clientConfig.getServerHost() + ", port: " + clientConfig.getServerPort()
                    + ", address: " + clientConfig.getServerAddress());
        }
    }

    ///

    public static final class BuildMqtt3ClientCallable implements Callable<Mqtt3Client> {

        final ClientConfig clientConfig;

        public BuildMqtt3ClientCallable(@NotNull final ClientConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public Mqtt3Client call() throws IeRuntimeException {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "BuildMqtt3ClientCallable - on Thread: " + Thread.currentThread().getName());
            try {
                return MqttClient.builder()
                        .useMqttVersion3()
                        .identifier(clientConfig.identifier)
                        .serverHost(clientConfig.hostName)
                        .serverPort(clientConfig.portNumber)
                        //.sslWithDefaultConfig()
                        .addConnectedListener(new ConnectionStatusCallback())
                        .addDisconnectedListener(new ConnectionStatusCallback())
                        .build();
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "error on BuildMqtt3ClientCallable: " + cause.getLocalizedMessage());
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.BUILD_CLIENT);
            }
        }
    }

    ///

    public static final class Mqtt3ClientConnectionCallable implements Callable<Mqtt3ConnAck> {

        final Mqtt3Client theClient;

        public Mqtt3ClientConnectionCallable(@NotNull final Mqtt3Client theClient) {
            this.theClient = theClient;
        }

        @Override
        public Mqtt3ConnAck call() throws IeRuntimeException {
            try {
                final Mqtt3ConnAck connectionAck = theClient
                        //.toBlocking().connect();
                        .toBlocking()
                        .connectWith()
                        .cleanSession(true)
                        .keepAlive(10)
                        .send();

                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionCallable - on Thread: " + Thread.currentThread().getName());
                return connectionAck;
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "Mqtt3ClientConnectionCallable - Error on mqtt3Client.toBlocking().connect()");
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.CONNECTION_FAILURE);
//                ConnectionFailedException // if an error occurs before the Connect message could be sent
//                ConnectionClosedException // if the connection is closed after the Connect message has been sent but before a ConnAck message has been received
//                Mqtt3ConnAckException     // if the ConnAck message contained an error code (the ConnAck message is contained in the exception)
//                MqttClientStateException  // if the client is already connecting or connected
            }
        }
    }

    ///

    public static final class Mqtt3ClientDisconnectionCallable implements Callable<Boolean> {

        final Mqtt3Client theClient;

        public Mqtt3ClientDisconnectionCallable(@NotNull final Mqtt3Client theClient) {
            this.theClient = theClient;
        }

        @Override
        public Boolean call() throws IeRuntimeException {
            try {
                theClient.toBlocking().disconnect();
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientDisconnectionCallable - on Thread: " + Thread.currentThread().getName());
                return true;
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "error on Mqtt3ClientDisconnectionCallable: " + cause.getLocalizedMessage());
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.DISCONNECTION_FAILURE);
            }
        }
    }

    ///

    public static final class Mqtt3ClientPublishCallable implements Callable<Boolean> {

        final Mqtt3Client theClient;
        final Mqtt3Publish thePublish;

        public Mqtt3ClientPublishCallable(
                @NotNull final Mqtt3Client theClient, @NotNull final Mqtt3Publish thePublish) {
            this.theClient = theClient;
            this.thePublish = thePublish;
        }

        @Override
        public Boolean call() throws IeRuntimeException {
            try {
//                Mqtt3Publish thePublish = Mqtt3Publish.builder()
//                        .topic("test/topic")
//                        .qos(MqttQos.AT_LEAST_ONCE)
//                        .payload("payload".getBytes())
//                        .build();
                theClient.toBlocking().publish(thePublish);
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientPublishCallable - on Thread: " + Thread.currentThread().getName());
                return true;
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "error on Mqtt3ClientPublishCallable: " + cause.getLocalizedMessage());
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.PUBLISH_FAILURE);
            }
        }
    }

    public static final class Mqtt3ClientSubscribeCallable implements Callable<Boolean> {

        final Mqtt3Client theClient;
        final Mqtt3Subscribe theSubscribe;

        public Mqtt3ClientSubscribeCallable(
                @NotNull final Mqtt3Client theClient, @NotNull final Mqtt3Subscribe theSubscribe) {
            this.theClient = theClient;
            this.theSubscribe = theSubscribe;
        }

        @Override
        public Boolean call() throws IeRuntimeException {
            try {
//                Mqtt3Subscribe theSubscribe = Mqtt3Subscribe.builder()
//                        .topicFilter("test/topic")
//                        .qos(MqttQos.EXACTLY_ONCE)
//                        .build();
                theClient.toBlocking().subscribe(theSubscribe);
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientSubscribeCallable - on Thread: " + Thread.currentThread().getName());
                return true;
            }
            catch (Exception cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "error on Mqtt3ClientSubscribeCallable: " + cause.getLocalizedMessage());
                throw new IeRuntimeException(cause, AppConstants.HiveMqErrorCode.SUBSCRIBE_FAILURE);
            }
        }
    }

    ///

    public static final class Mqtt3ClientConnectionFunction
            implements Function<Mqtt3Client, IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>> {

        @Override
        public IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>> apply(@NonNull final Mqtt3Client mqtt3Client) throws Exception {
            try {
                final Mqtt3ConnAck connectionAck = new Mqtt3ClientConnectionCallable(mqtt3Client).call();
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionFunction - on Thread: " + Thread.currentThread().getName());
                return new IeApiResponse<>(
                        new IePair<>(mqtt3Client, connectionAck), null);
            }
            catch (IeRuntimeException cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "Mqtt3ClientConnectionFunction - Error");
                return new IeApiResponse<>(null, cause);
            }
        }
    }

    public static final class Mqtt3ClientConnectionFunction2
            implements Function<Mqtt3Client, IePair<Mqtt3Client, Mqtt3ConnAck>> {

        @Override
        public IePair<Mqtt3Client, Mqtt3ConnAck> apply(@NonNull final Mqtt3Client mqtt3Client) throws Exception {
            try {
                final Mqtt3ConnAck connectionAck = new Mqtt3ClientConnectionCallable(mqtt3Client).call();
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionFunction2 - on Thread: " + Thread.currentThread().getName());
                return new IePair<>(mqtt3Client, connectionAck);
            }
            catch (IeRuntimeException cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "Mqtt3ClientConnectionFunction2 - Error");
                throw cause;
            }
        }
    }

    ///

    public static final class Mqtt3ClientPublishFunction
            implements Function<IePair<Mqtt3Client, Mqtt3ConnAck>, IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>> {

        final Mqtt3Publish thePublish;

        public Mqtt3ClientPublishFunction(@NotNull final Mqtt3Publish thePublish) {
            this.thePublish = thePublish;
        }

        @Override
        public IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>> apply(@NonNull final IePair<Mqtt3Client, Mqtt3ConnAck> pair) throws Exception {
            try {
                final Boolean result = new Mqtt3ClientPublishCallable(pair.first, thePublish).call();
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientPublishFunction - on Thread: " + Thread.currentThread().getName());
                return new IeApiResponse<>(pair, null);
            }
            catch (IeRuntimeException cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "Mqtt3ClientPublishFunction - on Thread: " + Thread.currentThread().getName());
                return new IeApiResponse<>(pair, cause);
            }
        }
    }

    ///

    public static final class Mqtt3ClientSubscribeFunction
            implements Function<IePair<Mqtt3Client, Mqtt3ConnAck>, IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>> {

        final Mqtt3Subscribe theSubscribe;

        public Mqtt3ClientSubscribeFunction(@NotNull final Mqtt3Subscribe theSubscribe) {
            this.theSubscribe = theSubscribe;
        }

        @Override
        public IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>> apply(@NonNull final IePair<Mqtt3Client, Mqtt3ConnAck> pair) throws Exception {
            try {
                final Boolean result = new Mqtt3ClientSubscribeCallable(pair.first, theSubscribe).call();
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientSubscribeFunction - on Thread: " + Thread.currentThread().getName());
                return new IeApiResponse<>(pair, null);
            }
            catch (IeRuntimeException cause) {
                java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.SEVERE, "Mqtt3ClientSubscribeFunction - on Thread: " + Thread.currentThread().getName());
                return new IeApiResponse<>(pair, cause);
            }
        }
    }

    ///
    
    public static final class BuildMqtt3ClientTask extends AbstractRxTask<Mqtt3Client>  {

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
                    Single.fromCallable(new BuildMqtt3ClientCallable(clientConfig))
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.from(executorService))
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

    public static final class Mqtt3ClientConnectionTask
            extends AbstractRxTask<IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>>  {

        final ClientConfig clientConfig;
        final ExecutorService executorService;

        public Mqtt3ClientConnectionTask(
                @NotNull final ClientConfig clientConfig,
                @NotNull final ExecutorService executorService) {
            this.clientConfig = clientConfig;
            this.executorService = executorService;
        }

        @Override
        public void runTask() {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionTask - on Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            setDisposable(
                    Single.fromCallable(new BuildMqtt3ClientCallable(clientConfig))
                            .map(new Mqtt3ClientConnectionFunction())
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.from(executorService))
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

    ///

    public static final class Mqtt3ClientPublishTask
            extends AbstractRxTask<IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>>  {

        final ClientConfig clientConfig;
        final Mqtt3Publish thePublish;
        final ExecutorService executorService;

        public Mqtt3ClientPublishTask(
                @NotNull final ClientConfig clientConfig,
                @NotNull final Mqtt3Publish thePublish,
                @NotNull final ExecutorService executorService) {
            this.clientConfig = clientConfig;
            this.thePublish = thePublish;
            this.executorService = executorService;
        }

        @Override
        public void runTask() {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionTask - on Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            setDisposable(
                    Single.fromCallable(new BuildMqtt3ClientCallable(clientConfig))
                            .map(new Mqtt3ClientConnectionFunction2())
                            .map(new Mqtt3ClientPublishFunction(thePublish))
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.from(executorService))
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

    ///

    public static final class Mqtt3ClientSubscribeTask
            extends AbstractRxTask<IeApiResponse<IePair<Mqtt3Client, Mqtt3ConnAck>>>  {

        final ClientConfig clientConfig;
        final Mqtt3Subscribe theSubscribe;
        final ExecutorService executorService;

        public Mqtt3ClientSubscribeTask(
                @NotNull final ClientConfig clientConfig,
                @NotNull final Mqtt3Subscribe theSubscribe,
                @NotNull final ExecutorService executorService) {
            this.clientConfig = clientConfig;
            this.theSubscribe = theSubscribe;
            this.executorService = executorService;
        }

        @Override
        public void runTask() {
            java.util.logging.Logger.getLogger("HiveMqttHelper").log(Level.INFO, "Mqtt3ClientConnectionTask - on Thread: " + Thread.currentThread().getName());
            rxDisposeIfPossible();
            setDisposable(
                    Single.fromCallable(new BuildMqtt3ClientCallable(clientConfig))
                            .map(new Mqtt3ClientConnectionFunction2())
                            .map(new Mqtt3ClientSubscribeFunction(theSubscribe))
                            .subscribeOn(Schedulers.io())
                            .observeOn(Schedulers.from(executorService))
                            .subscribeWith(new ApiDisposableSingleObserver())
            );
        }
    }

}
