package com.wyh.flink.stream.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author: WangYuhang
 * @create: 2021-03-25 17:18
 **/
public class SlowPrintSink<T> extends RichSinkFunction<T> {

    private int size;
    private int i;
    private int seconds;

    public SlowPrintSink(int seconds) {
        this.seconds = seconds;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = this.getRuntimeContext();
        size = context.getNumberOfParallelSubtasks();
        i = context.getIndexOfThisSubtask();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        System.out.println("总线程-" + size + ", 当前线程-" + i + ":" + value);
        Thread.sleep(1000 * seconds);
    }
}
