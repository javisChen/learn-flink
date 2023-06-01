package learn.streaming.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class SinkFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                value -> "Number:" + value,
                100,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        DataStreamSource<String> dataGenSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGenSource");


        FileSink<String> sink = FileSink
                .forRowFormat(new Path("output/test.txt"), new SimpleStringEncoder<String>())
                // 输出文件配置：文件名前缀、后缀
                .withOutputFileConfig(new OutputFileConfig("flink", ".log"))
                // 文件分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd"))
                .withRollingPolicy(DefaultRollingPolicy
                        .builder()
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(new MemorySize(1024 * 1024))
                        .build())
                .build();
        dataGenSource
                .sinkTo(sink);

        env.execute();

    }
}
