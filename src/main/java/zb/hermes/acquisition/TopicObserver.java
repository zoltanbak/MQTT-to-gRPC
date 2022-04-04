package zb.hermes.acquisition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import org.eclipse.paho.client.mqttv3.MqttException;
import zb.hermes.util.Notification;

record TopicObserver(MqttSubscriber mqttSubscriber) implements Observer<Notification<String>> {
    private static final Logger _log = LoggerFactory.getLogger(TopicObserver.class);

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        System.out.println("Subscribed to Topics");
    }

    @Override
    public void onNext(@NonNull Notification<String> notification) {
        _log.info("onNext(), operation: " + notification.getOperation() +
                        " topic: " + notification.getItem());
        try {
            if (notification.getOperation() == Notification.Operation.ADD) {
                mqttSubscriber.subscribe(notification.getItem());
            } else if (notification.getOperation() == Notification.Operation.REMOVE) {
                mqttSubscriber.unsubscribe(notification.getItem());
            }
        } catch (MqttException e) {
            _log.error(e.getLocalizedMessage());
        }
    }

    @Override
    public void onError(@NonNull Throwable e) {
        _log.error("Error on Topics");
    }

    @Override
    public void onComplete() {
        _log.info("onComplete");
    }
}
