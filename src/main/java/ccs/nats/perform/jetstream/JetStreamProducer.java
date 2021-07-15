package ccs.nats.perform.jetstream;


import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.nats.data.LatencyMeasurePing;
import ccs.nats.data.LatencyMeasurePingSerializer;
import ccs.perform.util.CommonProperties;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;

public class JetStreamProducer {
    private static final Logger log = LoggerFactory.getLogger(JetStreamProducer.class);

    // ----- static methods -------------------------------------------------

    public static void main(String[] asArgs) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        SecureRandom rand = new SecureRandom();

        LatencyMeasurePingSerializer serializer = new LatencyMeasurePingSerializer();

        Connection nc = Nats.connect(CommonProperties.get("ccs.nats.url", "nats://localhost:4222"));
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStreamUtil.initStream(topic, jsm);

        JetStream jetStream = nc.jetStream();

        try {
            int seq = 0;
            for( int i=0 ; i != iter ; i++ ) {
                int cnt =0;
                long st = System.nanoTime();
                long et = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    try {
                        jetStream.publish(topic, serializer.serialize(null, new LatencyMeasurePing(seq)));
                        seq++;
                        cnt++;
                    }catch(IllegalStateException e) {
                        // NOTE: publishのjavadocに従い、再接続時のバッファオーバーフロー対処
                        log.debug("publish failed.");
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                }

                log.info("{}: {} ns. {} times. {} ns/op", key, et-st, cnt, (et-st)/(double)cnt);
            }
        }catch(Throwable th) {
            log.error("occ exception.", th);

        }finally {
            nc.close();
        }

    }
}
