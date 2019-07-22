package org.apache.spark.network.custom;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

import java.util.Arrays;

public class CustomMessage extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final String[] indexIds;

    public CustomMessage(String appId, String execId, String[] blockIds) {
        this.appId = appId;
        this.execId = execId;
        this.indexIds = blockIds;
    }

    @Override
    protected Type type() { return Type.OPEN_BLOCKS; }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId) * 41 + Arrays.hashCode(indexIds);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockIds", Arrays.toString(indexIds))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof CustomMessage) {
            CustomMessage o = (CustomMessage) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Arrays.equals(indexIds, o.indexIds);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.StringArrays.encodedLength(indexIds);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.StringArrays.encode(buf, indexIds);
    }

    public static CustomMessage decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        return new CustomMessage(appId, execId, blockIds);
    }
}