
package ccs.nats.data;

import java.util.Base64;

public class LatencyMeasurePingDeserializer {

    public LatencyMeasurePing deserialize(String topic, byte[] data) {

        int seq = deserializeInt(data, 0);
        long tick = deserializeLong(data, 4);
        return new LatencyMeasurePing(seq, tick);
    }

    public LatencyMeasurePing deserializeBase64(Object object, String base64) {
        byte[] buf = Base64.getDecoder().decode(base64);
        return deserialize(null, buf);
    }

    private int deserializeInt(byte[] data, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            value |= data[i + offset] & 0xFF;
        }
        return value;
    }
    private long deserializeLong(byte[] data, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value <<= 8;
            value |= data[i + offset] & 0xFF;
        }
        return value;
    }

}
