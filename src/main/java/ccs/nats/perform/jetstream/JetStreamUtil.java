package ccs.nats.perform.jetstream;

import java.io.IOException;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamConfiguration;

public class JetStreamUtil {
    static void initStream(String topic, JetStreamManagement jsm) throws IOException, JetStreamApiException {
        if( !jsm.getStreamNames().contains(topic) ) {
            StreamConfiguration jstreamConfig = StreamConfiguration.builder()
                    .name(topic)
                    .build();
            jsm.addStream(jstreamConfig);
        }
    }
}
