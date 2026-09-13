package gc.garcol.pricestreaming.centrifugo;

import centrifugal.centrifugo.api.Api;
import centrifugal.centrifugo.api.CentrifugoApiGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@ConditionalOnProperty(name = "centrifugo.transport", havingValue = "grpc")
public class CentrifugoGrpcTransport implements CentrifugoTransport {

    private static final Metadata.Key<String> API_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final CentrifugoProperties properties;
    private final ObjectMapper objectMapper;
    private final ManagedChannel channel;
    private final CentrifugoApiGrpc.CentrifugoApiBlockingStub stub;

    public CentrifugoGrpcTransport(CentrifugoProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        CentrifugoProperties.Grpc grpc = properties.getGrpc();
        this.channel = ManagedChannelBuilder.forAddress(grpc.getHost(), grpc.getPort())
                .usePlaintext()
                .build();
        Metadata metadata = new Metadata();
        metadata.put(API_KEY, "apikey " + properties.getApiKey());
        this.stub = CentrifugoApiGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        log.info("Centrifugo gRPC transport targeting {}:{}", grpc.getHost(), grpc.getPort());
    }

    @Override
    public void publish(String channelName, List<?> payload) {
        byte[] data = objectMapper.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        Api.PublishRequest request = Api.PublishRequest.newBuilder()
                .setChannel(channelName)
                .setData(ByteString.copyFrom(data))
                .build();
        Api.PublishResponse response = stub
                .withDeadlineAfter(properties.getGrpc().getDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .publish(request);
        if (response.hasError()) {
            throw new IllegalStateException("centrifugo grpc publish failed: code=%d message=%s"
                    .formatted(response.getError().getCode(), response.getError().getMessage()));
        }
    }

    @Override
    public CentrifugoTransportType type() {
        return CentrifugoTransportType.GRPC;
    }

    @PreDestroy
    void shutdown() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
