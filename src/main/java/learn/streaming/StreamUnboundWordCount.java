package learn.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * 单词计数之滑动窗口计算
 */
@Slf4j
public class StreamUnboundWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf);

        // 2.读取数据：从文件读
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.31.21", 7777);

        // 3.处理数据：切分、转换、分组、聚合
        socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> collector) -> {
                            String[] splits = value.split("\\s");
                            for (String word : splits) {
                                collector.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                // 解决lambda泛型问题çç
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(wordWithCount -> wordWithCount.f0)
                .sum(1)
                .writeToSocket("192.168.31.21", 9091, stringIntegerTuple2 -> {
                    String s = stringIntegerTuple2.toString ();
                    System.out.println("发送数据：" + s);
                    return s.getBytes(StandardCharsets.UTF_8);
                });

        //这一行代码一定要实现，否则程序不执行
        env.execute();

    }

}