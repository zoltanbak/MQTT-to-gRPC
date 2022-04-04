package zb.hermes.configuration.topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import zb.hermes.configuration.TopicGrpc;
import zb.hermes.configuration.TopicRequest;

import java.util.HashSet;

public class Receiver extends TopicGrpc.TopicImplBase {
    private final HashSet<String> topics = new HashSet<>();
    private final Logger _log = LoggerFactory.getLogger(Receiver.class);

    @Override
    public void createNewTopic(TopicRequest request, StreamObserver<Empty> responseObserver) {
        final String topic = request.getTopic();
        _log.info("Received new Topic: " + topic);
        if (Holder.getInstance().addTopic(topic)) {
            _log.info("Topic: " + topic + " already exists!");
        }

        _log.info("CreateNewTopic onNext");
        responseObserver.onNext(Empty.newBuilder().build());
        _log.info("CreateNewTopic onCompleted");
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTopic(TopicRequest request, StreamObserver<Empty> responseObserver) {
        final String topic = request.getTopic();
        _log.info("Deleting Topic: " + topic);

        if(Holder.getInstance().removeTopic(topic)) {
            _log.info("DeleteTopic onNext");
            responseObserver.onNext(Empty.newBuilder().build());
            _log.info("DeleteTopic onCompleted");
            responseObserver.onCompleted();
        } else {
            _log.warn("Cannot remove Topic: " + topic);
            responseObserver.onError(Status
                    .NOT_FOUND
                    .withDescription("Cannot remove not existing Topic: " + topic)
                    .asRuntimeException());
        }
    }
}
