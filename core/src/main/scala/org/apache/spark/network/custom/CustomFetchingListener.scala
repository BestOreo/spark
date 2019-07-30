package org.apache.spark.network.custom

import java.util.EventListener

class CustomFetchingListener extends EventListener{
  def onFetchSuccess(blockId: String, data: String): Unit = {

  }

  /**
    * Called at least once per block upon failures.
    */
  def onFetchFailure(blockId: String, exception: Throwable): Unit = {

  }
}
