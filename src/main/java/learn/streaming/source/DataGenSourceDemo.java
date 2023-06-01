package learn.streaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                value -> "Number:" + value,
                100,
                RateLimiterStrategy.perSecond(5),
                Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGenSource")
                .print();

        env.execute();
    }
}
