package zb.hermes.acquisition;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zb.hermes.database.SensorDatabaseCommunicator;
import zb.hermes.sensor.SensorType;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import static zb.hermes.sensor.SensorType.*;

public class MqttCallbackImpl implements MqttCallback {
    private static final HashMap<String, SensorType> sensorTypeMap = new HashMap<>();
    private final Logger _log = LoggerFactory.getLogger(MqttCallbackImpl.class);

    static {
        sensorTypeMap.put("sensors/temperature", TEMPERATURE);
        sensorTypeMap.put("sensors/humidity", HUMIDITY);
        sensorTypeMap.put("sensors/pressure", PRESSURE);
        sensorTypeMap.put("sensors/ecg", ECG);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("MQTT connection lost because: " + cause);
    }

    @Override
    public void messageArrived(final String topic, MqttMessage message) throws Exception {
        final String payload = new String(message.getPayload());
        final JSONObject jsonObject = new JSONObject(payload);
        _log.info(String.format("Message arrived on thread [%d]: [%s][%s] %s",
                        Thread.currentThread().getId(),
                        LocalDateTime.now().toString(),
                        topic,
                        payload));

        final UUID mockCustomerId = UUID.randomUUID();

        Date timestamp = new Date();
        SensorDatabaseCommunicator.getInstance().send(
                String.valueOf(mockCustomerId),
                jsonObject.getString("id"),
                sensorTypeMap.get(topic),
                jsonObject.getLong("timestamp") * 1000,
                timestamp.getTime(),
                jsonObject.getInt("data"));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        _log.info("Mqtt delivery completed on " + Thread.currentThread());
    }
}
