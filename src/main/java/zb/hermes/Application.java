package zb.hermes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.client.mqttv3.MqttException;
import zb.hermes.acquisition.MqttSubscriber;
import zb.hermes.configuration.topic.Holder;
import zb.hermes.database.SensorDatabaseCommunicator;

public class Application {
    public static void main(String[] args) throws MqttException, InterruptedException {
        final Logger log = LoggerFactory.getLogger(Application.class);
        log.info("Main started on thread: " + Thread.currentThread());

        SensorDatabaseCommunicator sensorDatabase = SensorDatabaseCommunicator.getInstance();
        sensorDatabase.start("localhost", 18086);

        MqttSubscriber mqttSubscriber = MqttSubscriber.getInstance();

        Holder configurationHolder = Holder.getInstance();

        configurationHolder.subscribe(mqttSubscriber.getTopicObserver());

        configurationHolder.addTopic("sensors/temperature");
        configurationHolder.addTopic("sensors/humidity");
        configurationHolder.addTopic("sensors/pressure");
    }
}
