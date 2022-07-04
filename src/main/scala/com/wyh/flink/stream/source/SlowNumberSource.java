package com.wyh.flink.stream.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author: WangYuhang
 * @create: 2021-03-25 19:35
 **/
public class SlowNumberSource extends RichParallelSourceFunction<String> {

    private int size;
    private int n;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = this.getRuntimeContext();
        // task 并行度
        size = context.getNumberOfParallelSubtasks();
        // 当前 Subtask 下标
        n = context.getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        int i = 0;
        while (true){

            sourceContext.collect("task数: " + size + ", 当前subtask: " + n + ", value = " + i++);

            Thread.sleep(1000 * 5);

        }

    }

    @Override
    public void cancel() {

    }
}
