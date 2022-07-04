package com.wyh.flink.table

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author WangYuhang
 * @since 2022-06-16 14:11
 * */
object TableStreamingJob {
  case class Person(cla: Int, name: String, age: Int)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val personA = env.fromCollection(Seq(
      Person(1, "aaa", 7),
      Person(1, "bbb", 8),
      Person(2, "ccc", 14)
    ))
    val personB = env.fromCollection(Seq(
      Person(2, "ddd", 15),
      Person(3, "eee", 21),
      Person(4, "fff", 31)
    ))

    val tableA = tableEnv.fromDataStream(personA)
    tableEnv.createTemporaryView("person_b", personB)

    val result = tableEnv.sqlQuery(
      s"""
         |select * from $tableA where age > 10
         |union all
         |select * from person_b where age < 20
         |""".stripMargin)

//    val ds = tableEnv.toAppendStream[Person](result)
    val ds = tableEnv.toDataStream(result, classOf[Person])
    ds.print()


    tableEnv



    env.execute()
  }
}
