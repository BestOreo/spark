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

package org.apache.spark.network.custom

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.NettyBlockRpcServer
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer


class CustomNettyRpcServer (
       appId: String,
       serializer: Serializer,
       blockManager: BlockDataManager,
       answer: String)
  extends NettyBlockRpcServer(appId, serializer, blockManager) with Logging {

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    println("CustomNettyRpcServer receiving " + message)
    message match {
      case openBlocks: OpenBlocks =>
        println("message matches OpenBlocks")
        responseContext.onSuccess(ByteBuffer.wrap(answer.getBytes()))
    }
  }

}
