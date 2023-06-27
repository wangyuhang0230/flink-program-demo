package com.wyh.flink.doris

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.doris.flink.cfg.DorisExecutionOptions
import org.apache.doris.flink.cfg.DorisOptions
import org.apache.doris.flink.cfg.DorisReadOptions
import org.apache.doris.flink.sink.DorisSink
import org.apache.doris.flink.sink.writer.RowDataSerializer
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}

import java.util.Properties

/**
 * @author WangYuhang
 * @since 2023-06-26 16:32
 * */
object FlinkToDorisJob {
  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    // 非必须
    // conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    // 自定义端口
    conf.setInteger(RestOptions.PORT, 8081)
    // 带有 web UI 的本地 env
    // 访问地址：http://localhost:8081/
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.enableCheckpointing(10000)
    // using batch mode for bounded data
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)



    //mock rowdata source
    val source = env.fromElements("")
      .map(new MapFunction[String, RowData]() {
        @throws[Exception]
        override def map(value: String): RowData = {
          val genericRowData = new GenericRowData(7)
          genericRowData.setField(0, StringData.fromString("2023-06-26 16:18:06"))
          genericRowData.setField(1, StringData.fromString("16_fj_flxt_yp3bjdjdl"))
          genericRowData.setField(2, StringData.fromString("2023-06-26 16:20:33"))
          genericRowData.setField(3, 2.28)
          genericRowData.setField(4, null)
          genericRowData.setField(5, null)
          genericRowData.setField(6, null)
          genericRowData
        }
      })

    //doris sink option//doris sink option
    val builder = DorisSink.builder[RowData]
    val dorisBuilder = DorisOptions.builder
    dorisBuilder
      .setFenodes("10.10.53.31:8030")
      .setTableIdentifier("hntest.his_01009_2023")
      .setUsername("root")
      .setPassword("Thtf600100")

    // json format to streamload
    val properties = new Properties()
    properties.setProperty("format", "json")
    properties.setProperty("read_json_by_line", "true")

    val executionBuilder = DorisExecutionOptions.builder
    executionBuilder
      .setLabelPrefix("label-doris") //streamload label prefix
      .setStreamLoadProp(properties) //streamload params


    //flink rowdata‘s schema
    val fields = Array("update_time", "point_id", "upload_time", "val", "quality_type", "valid_range", "zero_drift")
    val types = Array(DataTypes.VARCHAR(256), DataTypes.VARCHAR(256), DataTypes.VARCHAR(256), DataTypes.DOUBLE, DataTypes.INT, DataTypes.VARCHAR(256), DataTypes.DOUBLE)

    builder
      .setDorisReadOptions(DorisReadOptions.builder.build)
      .setDorisExecutionOptions(executionBuilder.build)
      .setSerializer(
        RowDataSerializer
          .builder //serialize according to rowdata
          .setFieldNames(fields)
          .setType("json") //json format
          .setFieldType(types).build)
      .setDorisOptions(dorisBuilder.build)


    source.print()
    source.sinkTo(builder.build)


    env.execute("Flink Streaming Scala API Skeleton")



  }
}
