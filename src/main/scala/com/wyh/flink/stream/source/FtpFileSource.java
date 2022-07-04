package com.wyh.flink.stream.source;

import com.jcraft.jsch.ChannelSftp;
import com.wyh.flink.utils.SSH2Util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.InputStream;
import java.util.Vector;

/**
 * @author: WangYuhang
 * @create: 2021-03-25 19:35
 **/
public class FtpFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    private int taskSize;
    private int taskIndex;
    private ChannelSftp sftpChanne;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = this.getRuntimeContext();
        // task 并行度
        taskSize = context.getNumberOfParallelSubtasks();
        // 当前 Subtask 下标
        taskIndex = context.getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String[] stations = parameterTool.get("hn.stations").split(",");

        for (int index = taskIndex; index < stations.length; index += taskSize) {
            try {
                // 断点续传目录
                String path = "/" + stations[index] + "/" + parameterTool.get("hn.transmission.file-path") + "/";
                // 获取 sftp 通道
                // TODO 连接失败要不要重试
                sftpChanne = SSH2Util.getSftpChanne(
                        parameterTool.get("hn.sftp.host"),
                        parameterTool.getInt("hn.sftp.port"),
                        stations[index],
                        stations[index]);

                Vector files = sftpChanne.ls(path);
                for (Object object : files) {
                    ChannelSftp.LsEntry file = (ChannelSftp.LsEntry) object;
                    if (!file.getAttrs().isDir()) {
                        InputStream inputStream = sftpChanne.get(path + file.getFilename());
                        LineIterator iterator = IOUtils.lineIterator(inputStream, "UTF-8");
                        while (iterator.hasNext()) {
                            Tuple2<String, String> tuple2 = new Tuple2<>();
                            tuple2.setFields(stations[index], iterator.next());
                            sourceContext.collect(tuple2);
                        }
                    }
                }
                SSH2Util.disconnect(sftpChanne);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                SSH2Util.disconnect(sftpChanne);
            }
        }

    }

    @Override
    public void cancel() {
        SSH2Util.disconnect(sftpChanne);
    }
}
