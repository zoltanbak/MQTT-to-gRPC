package zb.hermes.configuration.topic;

//import io.reactivex.rxjava3.core.Notification;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import zb.hermes.util.Notification;
import zb.hermes.util.Notification.Operation;

import java.util.HashSet;

public class Holder {
    private static final Holder _instance;

    private final HashSet<String> _topics;
    private final Object _topicLock;
    private final PublishSubject<Notification<String>> _topicObservable;

    private Holder() {
        _topics = new HashSet<>();
        _topicLock = new Object();
        _topicObservable = PublishSubject.create();
        System.out.println("Configuration holder started on thread:" + Thread.currentThread().toString());
    }

    static {
        _instance = new Holder();
    }

    public static Holder getInstance() {
        return _instance;
    }

    public boolean addTopic(String topic) {
        synchronized (_topicLock) {
            if (_topics.add(topic)) {
                _topicObservable.onNext(new Notification<String>(Operation.ADD, topic));
                System.out.println("Topic: " + topic + " added successfully!");
                return true;
            }

            System.out.println("Topic: " + topic + " already exists!");
            return false;
        }
    }

    public HashSet<String> getTopics() {
        synchronized (_topicLock) {
            return _topics;
        }
    }

    public boolean removeTopic(String topic) {
        synchronized (_topicLock) {
            if (_topics.remove(topic)) {
                _topicObservable.onNext(new Notification<String>(Operation.REMOVE, topic));
                System.out.println("Topic: " + topic + " removed successfully!");
                return true;
            }

            System.out.println("Failed to remove topic: " + topic);
            return false;
        }
    }

    public Observable<Notification<String>> getTopicObservable() {
        return _topicObservable;
    }

    public void subscribe(Observer<Notification<String>> observer) {
        _topicObservable.subscribe(observer);
    }
}
