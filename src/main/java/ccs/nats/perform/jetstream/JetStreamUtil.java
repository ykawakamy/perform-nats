package ccs.nats.perform.jetstream;

import java.io.IOException;
import java.time.Duration;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

public class JetStreamUtil {
    static void initStream(String topic, JetStreamManagement jsm) throws IOException, JetStreamApiException {
        if( !jsm.getStreamNames().contains(topic) ) {
            StreamConfiguration jstreamConfig = StreamConfiguration.builder()
                    .name(topic)
                    .discardPolicy(DiscardPolicy.Old)
                    .maxMessages(1_000_000_000) // 1G msgs
                    .maxAge(Duration.ofSeconds(20))
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .build();
            jsm.addStream(jstreamConfig);
        }
    }
}
