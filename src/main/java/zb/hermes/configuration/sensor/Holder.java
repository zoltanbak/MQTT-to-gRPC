package zb.hermes.configuration.sensor;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;

import org.apache.commons.lang3.tuple.Pair;

import zb.hermes.sensor.SensorType;
import zb.hermes.util.Notification;

import java.util.HashMap;
import java.util.UUID;

public class Holder {
    private static final Holder _instance;
    private final HashMap<UUID, SensorType> _sensorConfig;
    private final Object _sensorConfigLock;
    private final PublishSubject<Notification<Pair<UUID, SensorType>>> _sensorConfigObservable;

    private Holder() {
        _sensorConfig = new HashMap<>();
        _sensorConfigLock = new Object();
        _sensorConfigObservable = PublishSubject.create();
        System.out.println("Sensor configuration holder started on thread:" + Thread.currentThread().toString());
    }

    static {
        _instance = new Holder();
    }

    public static Holder getInstance() {
        return _instance;
    }

    public boolean addSensorConfig(UUID id, SensorType type) {
        synchronized (_sensorConfigLock) {
            if (!_sensorConfig.containsKey(id)) {
                _sensorConfig.put(id, type);
                _sensorConfigObservable.onNext(new Notification<>(Notification.Operation.ADD, Pair.of(id, type)));
                System.out.println("Sensor config with - id: " + id + ", type: " + type + " added successfully!");
                return true;
            }

            System.out.println("Sensor config already existing! Id: " + id + ", type: " + type);
            return false;
        }
    }

    public HashMap<UUID, SensorType> getSensorConfig() {
        synchronized (_sensorConfigLock) {
            return _sensorConfig;
        }
    }

    public boolean removeSensorConfig(UUID id, SensorType type) {
        synchronized (_sensorConfigLock) {
            if (_sensorConfig.containsKey(id)) {
                _sensorConfig.remove(id);
                _sensorConfigObservable.onNext(new Notification<>(Notification.Operation.REMOVE, Pair.of(id, type)));
                System.out.println("SensorConfig - Id: " + id + ", type: " + type + " removed successfully!");
                return true;
            }

            System.out.println("Failed to remove sensor config: " + id);
            return false;
        }
    }

    public Observable<Notification<Pair<UUID, SensorType>>> getSensorConfigObservable() {
        return _sensorConfigObservable;
    }

    public void subscribe(Observer<Notification<Pair<UUID, SensorType>>> observer) {
        _sensorConfigObservable.subscribe(observer);
    }
}
