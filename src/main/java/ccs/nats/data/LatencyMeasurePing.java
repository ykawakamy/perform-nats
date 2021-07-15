
package ccs.nats.data;

import java.io.Serializable;

public class LatencyMeasurePing implements Serializable {
    int seq;
    long sendTick;
    long latency = -1;

    public LatencyMeasurePing(int seq) {
        this.seq = seq;
        // Note : System.nanoTime()はJVMごとで別の値を取るため。
        sendTick = System.currentTimeMillis();
    }

    public LatencyMeasurePing(int seq, long tick) {
        this.seq = seq;
        this.sendTick = tick;
        this.latency = System.currentTimeMillis() - sendTick;
    }

    public int getSeq() {
        return seq;
    }

    public void resetLatency() {
        this.latency = System.currentTimeMillis() - sendTick;
    }

    public long getLatency() {
        return latency;
    }

}
