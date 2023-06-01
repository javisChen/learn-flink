package learn.streaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.
                <String>builder()
                .setBootstrapServers("") // 设置kafka服务器
                .setGroupId("")
                .setTopics("test") // 主题
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .print()
                .setParallelism(1);

        env.execute();
    }
}
