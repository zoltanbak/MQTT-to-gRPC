package zb.hermes.database;

import com.google.protobuf.Empty;
import io.grpc.StatusRuntimeException;
import zb.hermes.sensor.SensorType;
import zb.hermes.util.GrpcClient;

import java.util.concurrent.atomic.AtomicBoolean;

public class SensorDatabaseCommunicator extends GrpcClient {
    private static final SensorDatabaseCommunicator _instance;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private SensorDatabaseCommunicator() {
        super();
        _log.info("SensorDatabaseCommunicator created on thread: " + Thread.currentThread());
    }

    static {
        _instance = new SensorDatabaseCommunicator();
    }

    public static SensorDatabaseCommunicator getInstance() {
        return _instance;
    }

    public void start(final String ip, final int port) {
        super.start(ip, port);
        _log.info("SensorDatabaseCommunicator started on thread: " + Thread.currentThread() +
                ", address: " + _ip + ":" + port);
        running.set(true);
    }

    public void shutdown() {
        running.set(false);
        super.shutdown();
    }

    public void send(String customerId,
                     String sensorId,
                     SensorType sensorType,
                     long sensorTimestamp,
                     long acquisitionTimestamp,
                     int data) throws Exception {
        if(running.get()) {
            SensorDatabaseServiceGrpc.SensorDatabaseServiceBlockingStub stub = SensorDatabaseServiceGrpc.newBlockingStub(_channel);

            CreateRequest request = CreateRequest.newBuilder()
                    .setCustomerId(customerId)
                    .setSensorId(sensorId)
                    .setSensorType(sensorType)
                    .setSensorTimestamp(sensorTimestamp)
                    .setAcquisitionTimestamp(acquisitionTimestamp)
                    .setData(data)
                    .build();

            _log.info("Sending data to sensor database: " + request);

            try {
                final Empty response = stub.create(request);
                _log.info("Response: " + response.toString());
            } catch (StatusRuntimeException e) {
                // TODO: Buffer it, or use a buffer and flush it from another thread
                _log.error(e.toString());
            }
        } else {
            throw new Exception("Failed to send database, because communicator has not started yet!");
        }
    }
}
