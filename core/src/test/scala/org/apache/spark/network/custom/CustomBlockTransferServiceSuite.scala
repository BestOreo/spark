package org.apache.spark.network.custom

import java.nio.ByteBuffer
import java.util
import java.util.LinkedHashMap

import com.google.common.collect.Maps
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.ShuffleBlockId
import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.{BeforeAndAfterEach, Matchers}


class CustomBlockTransferServiceSuite extends SparkFunSuite
  with BeforeAndAfterEach
  with Matchers {

  private var transferService: CustomBlockTransferService = _
  val answer = "Hello CustomBlockTransferService"

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
    transferService = createService(port = 0)
    val blockId = ShuffleBlockId(1,2,3)
    val listener = mock(classOf[BlockFetchingListener])

    transferService.fetchBlocks("localhost", transferService.port, "1",
      Array(blockId.toString), listener, null)

    Thread.sleep(1000)
    assert(answer == CustomOneForOneBlockFetcher.fetchResult)
  }

  private def createService(port: Int): CustomBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new CustomBlockTransferService(conf, securityManager,
      "localhost", "localhost", port, 1, answer)
    service.init(blockDataManager)
    service
  }
  
}
