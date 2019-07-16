package org.apache.spark.network.custom;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.network.shuffle.OneForOneBlockFetcher;
import org.apache.spark.network.util.TransportConf;


public class CustomOneForOneBlockFetcher extends OneForOneBlockFetcher {

    public CustomOneForOneBlockFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] blockIds,
            BlockFetchingListener listener,
            TransportConf transportConf,
            DownloadFileManager downloadFileManager) {
        super(client, appId, execId, blockIds, listener, transportConf, downloadFileManager);
    }

    @Override
    public void start() {
        super.start();
    }

}
