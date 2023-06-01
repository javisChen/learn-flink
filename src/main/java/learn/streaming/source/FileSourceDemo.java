package learn.streaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), Path.fromLocalFile(new File("input/word.txt")))
                .build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .print()
                .setParallelism(1);

        env.execute();
    }
}
