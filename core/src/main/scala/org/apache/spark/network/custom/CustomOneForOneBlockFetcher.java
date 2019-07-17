package org.apache.spark.network.custom;

import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.network.shuffle.OneForOneBlockFetcher;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.util.TransportConf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class CustomOneForOneBlockFetcher{

    private final TransportClient client;
    private final OpenBlocks openMessage;
    private final String[] blockIds;
    private final BlockFetchingListener listener;

    static String fetchResult = "nothing happens";

    public CustomOneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener) {
        this.client = client;
        this.openMessage = new OpenBlocks(appId, execId, blockIds);
        this.blockIds = blockIds;
        this.listener = listener;
    }

    public void start(){
        client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                fetchResult = StandardCharsets.UTF_8.decode(response).toString();
                System.out.println("sendRPC response: " + fetchResult);
            }

            @Override
            public void onFailure(Throwable e) {
                System.out.println("sendRPC fails :");
                e.printStackTrace();
            }
        });
    }
}
