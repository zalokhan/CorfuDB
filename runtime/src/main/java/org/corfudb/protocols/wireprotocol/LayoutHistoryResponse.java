package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.Layout;

import java.util.List;

/**
 * Created by zlokhandwala on 5/22/17.
 */
@Data
@AllArgsConstructor
public class LayoutHistoryResponse implements ICorfuPayload<LayoutHistoryResponse> {

    private List<Layout> layoutList;

    public LayoutHistoryResponse(ByteBuf buf) {
        layoutList = ICorfuPayload.listFromBuffer(buf, Layout.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, layoutList);
    }
}
