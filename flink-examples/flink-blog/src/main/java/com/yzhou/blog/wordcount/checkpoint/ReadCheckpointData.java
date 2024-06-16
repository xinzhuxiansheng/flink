package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadCheckpointData {
    public static void main(String[] args) throws Exception {
        String metadataPath = "D:\\TMP\\7caba09b0eea52c93cbaf809a3a2c2fa\\chk-1";

//        CheckpointMetadata metadataOnDisk = SavepointLoader.loadSavepointMetadata(metadataPath);
//        System.out.println("checkpointId: " + metadataOnDisk.getCheckpointId());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SavepointReader savepoint = SavepointReader.read(
                env,
                metadataPath,
                new HashMapStateBackend());
        // 定义 KeyedStateReaderFunction 读取状态
        DataStream<KeyedState> keyedCountState = savepoint.readKeyedState(
                "wc-sum",
                new ReaderFunction());

//        keyedCountState.addSink(new SinkFunction<KeyedState>() {
//            @Override
//            public void invoke(KeyedState value, Context context) throws Exception {
//                //SinkFunction.super.invoke(value, context);
//                System.out.println(value.key + " , " + value.value);
//            }
//        });
        keyedCountState.print();
        env.execute();
    }
}
