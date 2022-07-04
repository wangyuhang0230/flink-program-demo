package com.wyh.flink.stream.sink;

import com.wyh.flink.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author WangYuhang
 * @since 2021-03-25 17:18
 **/
public class PhoenixSink extends RichSinkFunction<Tuple2<String, String>> {

    private static Logger logger = LoggerFactory.getLogger(PhoenixSink.class);

    private int size;
    private int i;

    private Connection conn;
    private PreparedStatement pres;
    private String sql = "UPSERT INTO TEST.HN_TEST VALUES(?, ?)";

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = this.getRuntimeContext();
        size = context.getNumberOfParallelSubtasks();
        i = context.getIndexOfThisSubtask();

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        conn = PhoenixUtil.getConnection(parameterTool);
        pres = conn.prepareStatement(sql);
        logger.error("总线程-{}, 当前线程-{}: Phoenix 连接创建成功！", size, i);
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        pres.setTimestamp(1, new Timestamp(new Date().getTime()));
        pres.setString(2, value.f0 + "_" + value.f1);
        pres.executeUpdate();
        System.out.println("总线程-" + size + ", 当前线程-" + this.i + ":" + value + "");
        Thread.sleep(1000 * 3);
        conn.commit();
//        pres.executeBatch();
    }

    @Override
    public void close() throws Exception {
        super.close();
        PhoenixUtil.closeConn(pres, conn);
        logger.error("总线程-{}, 当前线程-{}: Phoenix 连接已关闭！", size, i);
    }
}
