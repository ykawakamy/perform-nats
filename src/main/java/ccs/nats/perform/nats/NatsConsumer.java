package ccs.nats.perform.nats;


import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.nats.data.LatencyMeasurePing;
import ccs.nats.data.LatencyMeasurePingDeserializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

public class NatsConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(NatsConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();

        Connection nc = Nats.connect(CommonProperties.get("ccs.nats.url", "nats://localhost:4222"));
        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        Subscription sub = nc.subscribe(topic);

        try {
            // トピックを指定してメッセージを送信する

            SequencialPerformCounter pc = new SequencialPerformCounter();
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;

                while( (et = System.nanoTime()) - st < loop_ns) {
                    Message msg = sub.nextMessage(Duration.ofMillis(100L));
                    byte[] data = msg != null ? msg.getData() : null;
                    if (data != null) {
                        LatencyMeasurePing ping = serializer.deserialize("", data);
                        pc.perform(ping.getSeq());
                        long latency = ping.getLatency();
                        pc.addLatency(latency);
                        hist.increament(latency);
                    }
                }

                PerformSnapshot snap = pc.reset();
                snap.print(log, et-st);
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            nc.close();
        }
    }

}
