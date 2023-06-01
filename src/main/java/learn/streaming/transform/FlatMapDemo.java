package learn.streaming.transform;

import learn.streaming.source.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if (value.getId().equals("s1")) {
                    out.collect(String.valueOf(value.getVc()));
                } else if (value.getId().equals("s2")) {
                    out.collect(String.valueOf(value.getTs()));
                    out.collect(String.valueOf(value.getVc()));
                }
            }
        }).print();

        env.execute();
    }
}
