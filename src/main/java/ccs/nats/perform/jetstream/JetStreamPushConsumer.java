package ccs.nats.perform.jetstream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.nats.data.LatencyMeasurePing;
import ccs.nats.data.LatencyMeasurePingDeserializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;

public class JetStreamPushConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(JetStreamPushConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();
        SequencialPerformCounter pc = new SequencialPerformCounter();

        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        Connection nc = Nats.connect(CommonProperties.get("ccs.nats.url", "nats://localhost:4222"));
        JetStream jetStream = nc.jetStream();

        Dispatcher dispatcher = nc.createDispatcher();
        MessageHandler handlar = (msg)->{
            byte[] data = msg != null ? msg.getData() : null;
            if (data != null) {
                LatencyMeasurePing ping = serializer.deserialize("", data);
                pc.perform(ping.getSeq());
                long latency = ping.getLatency();
                pc.addLatency(latency);
                hist.increament(latency);
            }
            msg.ack();
        };

        JetStreamSubscription sub = jetStream.subscribe(topic, dispatcher, handlar, true);
        try {
            // トピックを指定してメッセージを送信する

            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;


                while( (et = System.nanoTime()) - st < loop_ns) {
                    Thread.onSpinWait();
                }

                PerformSnapshot snap = pc.reset();
                log.info("{}: {} op, {} errors, {} ns/op, latency: {} ms/op", key, snap.getPerform(), snap.getErr(), snap.getElapsedPerOperation(et-st), snap.getLatencyPerOperation() );
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            nc.close();
        }
    }

}
