package ccs.nats.perform.nats;


import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.nats.data.LatencyMeasurePing;
import ccs.nats.data.LatencyMeasurePingDeserializer;
import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformCounterMap;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.TopicNameSupplier;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;

public class NatsDispatcherConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(NatsDispatcherConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String topicrange = System.getProperty("ccs.perform.topicrange", null);
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        TopicNameSupplier supplier = TopicNameSupplier.create(topic, topicrange);

        PerformCounterMap pc = new PerformCounterMap();
        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();

        Connection nc = Nats.connect(CommonProperties.get("ccs.nats.url", "nats://localhost:4222"));
        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        MessageHandler handler = new MessageHandler() {

            @Override
            public void onMessage(Message msg) throws InterruptedException {
                byte[] data = msg != null ? msg.getData() : null;
                if (data != null) {
                    LatencyMeasurePing ping = serializer.deserialize("", data);
                    String subject = msg.getSubject();
                    pc.perform( subject , ping.getSeq());
                    long latency = ping.getLatency();
                    pc.addLatency(subject, latency);
                    hist.increament(latency);
                }

            }
        };
        Dispatcher dispatcher = nc.createDispatcher();
        for ( var it : supplier.getAll() ) {
            dispatcher.subscribe(it, handler);
        }

        try {
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;
                TimeUnit.NANOSECONDS.sleep(loop_ns);
                et = System.nanoTime();

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
