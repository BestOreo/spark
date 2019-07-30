/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.network.custom;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class CustomOneForOneFetcher {
    private final TransportClient client;
    private final CustomMessage customMessage;
    private final String[] blockIds;
    private final CustomFetchingListener listener;

    public CustomOneForOneFetcher(
            TransportClient client,
            String appId,
            String execId,
            String[] indexIds,
            CustomFetchingListener listener) {
        this.client = client;
        this.customMessage = new CustomMessage(appId, execId, indexIds);
        this.blockIds = indexIds;
        this.listener = listener;
    }

    public void start(){
        client.sendRpc(customMessage.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                String fetchResult = StandardCharsets.UTF_8.decode(response).toString();
                System.out.println("--response: " + fetchResult);
                listener.onFetchSuccess(blockIds[0], fetchResult);
            }

            @Override
            public void onFailure(Throwable e) {
                System.out.println("sendRPC fails :");
                listener.onFetchFailure("fail", null);
                e.printStackTrace();
            }
        });
    }
}
