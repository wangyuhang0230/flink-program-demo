package com.wyh.flink.stream

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Random, UUID}

/**
 * @author WangYuhang
 * @since 2022-06-10 13:28
 * */
object WatermarkTestJob {
  def main(args: Array[String]): Unit = {

    //1.env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setStateBackend(new FsStateBackend("checkpoint"))
    env.setStateBackend(new HashMapStateBackend())

    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.enableUnalignedCheckpoints()
//    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("checkpoints"))

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000))


    //2.Source
    //模拟实时订单数据(数据有延迟和乱序)
    val orderDS = env.addSource(new SourceFunction[Order]() {
      var flag = true

      @Override
      def run(ctx: SourceContext[Order] ) {
        val random = new Random()
        while (flag) {
          val orderId = UUID.randomUUID().toString
          val userId = random.nextInt(3)
          val money = random.nextInt(100)
          //模拟数据延迟和乱序!
          val lateTime = random.nextInt(10) * 1000
          val eventTime = System.currentTimeMillis() - random.nextInt(10) * 1000
          val order = Order(orderId, userId, money, eventTime, lateTime)
          println(order)
          ctx.collect(order)

          TimeUnit.SECONDS.sleep(1)
        }
      }

      @Override
      def cancel() {
        flag = false
      }
    })


    //3.Transformation
    val watermakerDS = orderDS
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(3))
          .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
            override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
          })
      )

    //代码走到这里,就已经被添加上Watermaker了!接下来就可以进行窗口计算了
    //要求每隔5s,计算5秒内(基于时间的滚动窗口)，每个用户的订单总金额
    val outputTag = new OutputTag[Order]("Seriouslylate")

    val result = watermakerDS
      .keyBy(_.userId)
      //.timeWindow(Time.seconds(5), Time.seconds(5))
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(5))
      .sideOutputLateData(outputTag)
      .sum("money")


    val result2 = result.getSideOutput(outputTag)


    //4.Sink
    result.print("正常的数据和迟到不严重的数据")
    result2.print("迟到严重的数据")

    //5.execute
    env.execute()

  }
  case class Order(orderId: String,
              userId: Integer,
              money:Integer,
              eventTime: Long,
              currentTime: Long)
}
