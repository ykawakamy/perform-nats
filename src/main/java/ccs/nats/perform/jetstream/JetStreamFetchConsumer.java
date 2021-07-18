package ccs.nats.perform.jetstream;


import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.nats.data.LatencyMeasurePing;
import ccs.nats.data.LatencyMeasurePingDeserializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;

public class JetStreamFetchConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(JetStreamFetchConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();

        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        Connection nc = Nats.connect(CommonProperties.get("ccs.nats.url", "nats://localhost:4222"));
        JetStream jetStream = nc.jetStream();

        try {
            // トピックを指定してメッセージを送信する

            SequencialPerformCounter pc = new SequencialPerformCounter();
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;

                JetStreamSubscription sub = jetStream.subscribe(topic);
                while( (et = System.nanoTime()) - st < loop_ns) {
                    List<Message> msgs = sub.fetch(100, Duration.ofMillis(100L));
                    for (Message msg : msgs) {
                        byte[] data = msg != null ? msg.getData() : null;
                        if (data != null) {
                            LatencyMeasurePing ping = serializer.deserialize("", data);
                            pc.perform(ping.getSeq());
                            long latency = ping.getLatency();
                            pc.addLatency(latency);
                            hist.increament(latency);
                        }
                        msg.ack();
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
