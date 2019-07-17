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

import scala.collection.JavaConverters._

import org.apache.spark.network.client.TransportClientBootstrap
import org.apache.spark.network.{BlockDataManager, TransportContext}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network.netty.{NettyBlockRpcServer, NettyBlockTransferService}
import org.apache.spark.network.server.TransportServerBootstrap
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, RetryingBlockFetcher}


private[spark] class CustomBlockTransferService(
     conf: SparkConf,
     securityManager: SecurityManager,
     bindAddress: String,
     hostName: String,
     _port: Int,
     numCores: Int,
     answer: String)
  extends NettyBlockTransferService(conf, securityManager, bindAddress, hostName, _port, numCores) {

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new CustomNettyRpcServer(conf.getAppId, serializer, blockDataManager, answer)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)

    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new CustomOneForOneBlockFetcher(client, appId, execId, blockIds, listener).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

}
