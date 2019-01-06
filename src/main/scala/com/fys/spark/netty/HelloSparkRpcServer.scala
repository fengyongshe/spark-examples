package com.fys.spark.netty

import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.common.{RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import com.fys.spark.rpc.netty.NettyRpcEnvFactory

object HelloSparkRpcServer {

  def main(args: Array[String]): Unit = {

    val config = RpcEnvServerConfig(new RpcConf(),"Hello Server","localhost",52345)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("HelloService",helloEndpoint)
    rpcEnv.awaitTermination()

  }
}
