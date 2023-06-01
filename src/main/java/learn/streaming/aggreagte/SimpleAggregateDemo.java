package learn.streaming.aggreagte;

import learn.streaming.source.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 按照id分组
        // keyBy不是转换算子，只是对数据进行重分区，不能设置并行度
        // 分组与分区的关系：

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

//        SingleOutputStreamOperator<WaterSensor> sum = keyedStream.sum("vc");
//        SingleOutputStreamOperator<WaterSensor> sum = keyedStream.max("vc");

        // max/maxBy区别：
        // max：只会取比较字段的最大值，非比较字段取第一条的值
        // maxBy：取比较字段的最大值，同时非比较字段取最大值这条数据的值
        SingleOutputStreamOperator<WaterSensor> sum = keyedStream.maxBy("vc");

        sum.print();

        env.execute();

    }

}
