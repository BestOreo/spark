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

import org.apache.spark.network.BlockDataManager
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.storage.ShuffleBlockId
import org.mockito.Mockito.{mock, verify}
import org.scalatest.{BeforeAndAfterEach, Matchers}


class CustomTransferServiceSuite extends SparkFunSuite
  with BeforeAndAfterEach
  with Matchers {

  private var transferService: CustomTransferService = _
  private val expectedAnswer = "Hello CustomBlockTransferService"

  override def afterEach() {
    try {
      if (transferService != null) {
        transferService.close()
        transferService = null
      }
    } finally {
      super.afterEach()
    }
  }


  test("test procedure of custom transferService") {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    new SparkContext("local", "test", conf)
    val host = SparkEnv.get.rpcEnv.address.host

    // build a rpc server always replies (blockId.name, expectedAnswer)
    transferService = createService(conf)
    val blockId = ShuffleBlockId(1,2,3)
    val listener = mock(classOf[CustomFetchingListener])

    // client applies response from transferService rpc server
    transferService.customFetchData(host, transferService.port,
      "1", Array(blockId.toString), listener)
    // Here needs some time for communication between server and client
    Thread.sleep(1000)
    // verify whether the callback response is expectedAnswer
    verify(listener).onFetchSuccess(blockId.name, expectedAnswer)
  }

  private def createService(conf: SparkConf): CustomTransferService = {
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new CustomTransferService(conf)
    service.init(blockDataManager)
    service
  }

}
