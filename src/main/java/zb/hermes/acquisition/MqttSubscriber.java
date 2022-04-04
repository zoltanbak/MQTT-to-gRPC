package zb.hermes.acquisition;

import io.reactivex.rxjava3.core.Observer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zb.hermes.util.Notification;

import java.util.HashSet;

public class MqttSubscriber {
    private static MqttSubscriber instance;
    private static String protocol = "tcp";
    private static final String ip = "192.168.3.202";
    private static final int port = 1883;
    private final String clientId = "ToDoUuid_Mqtt_gRPC_bridge";
    private static MqttClient _client;
    private final int qos = 2;
    private final Logger _log;

    //    Observable<String> _topics;
    Observer<Notification<String>> _topics;

    private MqttSubscriber() throws Exception {
        _log = LoggerFactory.getLogger(MqttSubscriber.class);
        _log.info("MqttSubscriber is initializing on thread: " + Thread.currentThread());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);

        final String ADDRESS = protocol + "://" + ip + ":" + port;
        _client = new MqttClient(ADDRESS, clientId, new MemoryPersistence());
        _client.setCallback(new MqttCallbackImpl());
        _client.connect(options);

        _topics = new TopicObserver(this);

//        _topics = (Observable<String>) Holder.getInstance().getTopicObservable().subscribe(
//                notification -> {
//                    _log.info(
//                            "Operation: " + notification.getOperation() +
//                                    " topic: " + notification.getItem());
//                    if(notification.getOperation() == Notification.Operation.ADD) {
//                        subscribe(notification.getItem());
//                    } else if (notification.getOperation() == Notification.Operation.REMOVE) {
//                        unsubscribe(notification.getItem());
//                    }
//                }
//        );
    }

    static {
        try {
            instance = new MqttSubscriber();
        } catch (Exception e) {
            System.err.println("Failed to create MqttSubscriber on " + ip + ":" + port + ": " + e);
        }
    }

    public static MqttSubscriber getInstance() {
        return instance;
    }

    public void subscribe(final String topic) throws MqttException {
        _log.info("Subscribe to topic: " + topic);
        _client.subscribe(topic, qos);
    }

    public void subscribe(final HashSet<String> topics) throws MqttException {
        for (final String topic: topics) {
            _log.info("Subscribe to topic: " + topic);
            _client.subscribe(topic, qos);
        }
    }

    public void unsubscribe(final String topic) throws MqttException {
        _log.info("Unsubscribe from topic: " + topic);
        _client.unsubscribe(topic);
    }

    public Observer<Notification<String>> getTopicObserver() {
        return _topics;
    }
}
