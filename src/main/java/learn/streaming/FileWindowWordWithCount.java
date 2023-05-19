package learn.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * 单词计数之滑动窗口计算
 * <p>
 * Created by xuwei.tech
 */
public class FileWindowWordWithCount {

    public static void main(String[] args) throws Exception {
        //1、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、配置数据源读取数据
        DataStream<String> text = env.readTextFile("input");
        //3、进行一系列转换
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);
        //4、配置数据汇写出数据
//        counts.writeAsText("output");
        counts.print().setParallelism(1);
        //5、提交执行
        env.execute("Streaming WordCount");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 根据空格拆分字符串为单词
            String[] words = value.toLowerCase().split("\\s+");

            // 输出每个单词的频率为 1
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}