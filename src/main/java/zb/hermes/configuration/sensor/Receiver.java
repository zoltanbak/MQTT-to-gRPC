package zb.hermes.configuration.sensor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import zb.hermes.configuration.SensorConfigGrpc;
import zb.hermes.configuration.SensorConfigurationRequest;
import zb.hermes.sensor.SensorType;

import java.util.UUID;

public class Receiver extends SensorConfigGrpc.SensorConfigImplBase {
    private final Logger _log = LoggerFactory.getLogger(Receiver.class);

    @Override
    public void createNewSensor(SensorConfigurationRequest request, StreamObserver<Empty> responseObserver) {
        final UUID id = UUID.fromString(request.getId());
        final SensorType type = request.getType();

        _log.info("Received new sensor configuration - id: " + id + ", type: " + type);

        if (Holder.getInstance().addSensorConfig(id, type)) {
            _log.info("Sensor with id: " + id + "and type: " + type + " already exists!");
        }

        _log.info("CreateNewSensor onNext");
        responseObserver.onNext(Empty.newBuilder().build());
        _log.info("CreateNewSensor onCompleted");
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSensor(SensorConfigurationRequest request, StreamObserver<Empty> responseObserver) {
        final UUID id = UUID.fromString(request.getId());
        final SensorType type = request.getType();
        _log.info("Deleting sensor config with id: " + id + ", type: " + type);

        if(Holder.getInstance().removeSensorConfig(id, type)) {
            _log.info("DeleteSensor onNext");
            responseObserver.onNext(Empty.newBuilder().build());
            _log.info("DeleteSensor onCompleted");
            responseObserver.onCompleted();
        } else {
            _log.warn("Cannot remove sensor config with id: " + id + ", type: " + type);
            responseObserver.onError(Status
                    .NOT_FOUND
                    .withDescription("Cannot remove not existing sensor config with id: " + id + ", type: " + type)
                    .asRuntimeException());
        }
    }
}
