package com.fys.spark.netty

import com.fys.spark.rpc.common.{RpcCallContext, RpcEndpoint, RpcEnv}

class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("Start Hello Endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      println(s"receive $msg")
      context.reply(s"Hi, $msg")
    }
    case SayBye(msg) => {
      println(s"receive $msg")
      context.reply(s"byte, $msg")
    }
  }

  override def onStop(): Unit ={
    println("Stop, Hello Endpoint")
  }

}

case class SayHi(msg: String)
case class SayBye(msg: String)