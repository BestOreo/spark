package org.apache.spark.network.custom

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object CustomTransferConf {
  val RPC_SERVER_PORT: ConfigEntry[String] =
    ConfigBuilder("org.apache.spark.network.custom.customRpcServerPort")
      .doc("Custom Transfer Service Rpc port to receive message")
      .stringConf
      .createWithDefault("54345")

  val NUM_USABLE_CORES: ConfigEntry[String] =
    ConfigBuilder("org.apache.spark.network.custom.numUsableCores")
      .doc("The number of usable cores for custom Rpc Server")
      .stringConf
      .createWithDefault("2")
}
