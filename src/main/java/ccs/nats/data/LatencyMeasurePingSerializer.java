
package ccs.nats.data;

import java.util.Base64;

public class LatencyMeasurePingSerializer {

    public byte[] serialize(String topic, LatencyMeasurePing data) {
        if (data == null)
            return null;

        return new byte[] {
                //----
                (byte) (data.seq >>> 24),
                (byte) (data.seq >>> 16),
                (byte) (data.seq >>> 8),
                (byte) (data.seq),
                //----
                (byte) (data.sendTick >>> 56),
                (byte) (data.sendTick >>> 48),
                (byte) (data.sendTick >>> 40),
                (byte) (data.sendTick >>> 32),
                (byte) (data.sendTick >>> 24),
                (byte) (data.sendTick >>> 16),
                (byte) (data.sendTick >>> 8),
                (byte) (data.sendTick)
        };
    }

    public String serializeBase64(Object object, LatencyMeasurePing latencyMeasurePing) {
        byte[] data = serialize(null, latencyMeasurePing);

        if( data == null )
            return null;

        return Base64.getEncoder().encodeToString(data);
    }

}
