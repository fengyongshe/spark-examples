package com.fys.spark.netty

import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.common.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import com.fys.spark.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object HelloSparkRpcClient {

  def main(args: Array[String]): Unit = {

    import scala.concurrent.ExecutionContext.Implicits.global

    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "HelloClient")

    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(
      RpcAddress("localhost",52345),
      "HelloService")
    val future: Future[String] = endPointRef.ask[String](SayHi("Neo"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Get the result = $value")
      case scala.util.Failure(e) => println(s"Got Error: $e")
    }
    Await.result(future, Duration.apply("30s"))

  }
}
