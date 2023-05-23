package learn.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据：从文件读
        DataStreamSource<String> text = env.readTextFile("input/word.txt");

        // 3.处理数据：切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = text.flatMap(new FlatMapFunction
                <String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    // 转换成二元组（word，1）
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    // 通过采集器向下游发送数据
                    collector.collect(tuple2);
                }
            }
        });

        // 3.2 分组
        KeyedStream<Tuple2<String, Integer>, Object> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> wordWithCount) {
                return wordWithCount.f0;
            }
        });

        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = wordAndOneKS.sum(1);

        // 3.4 输出数据
        SerializationSchema<Tuple2<String, Integer>> schema = new SerializationSchema<Tuple2<String, Integer>>() {
            @Override
            public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
                String s = stringIntegerTuple2.toString();
                System.out.println("发送数据：" + s);
                return s.getBytes(StandardCharsets.UTF_8);
            }

        };
        streamOperator.writeToSocket("localhost", 9091, schema);

        //这一行代码一定要实现，否则程序不执行
        env.execute();

    }

}