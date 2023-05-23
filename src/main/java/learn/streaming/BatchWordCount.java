package learn.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词计数之滑动窗口计算
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //1、设置运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2、配置数据源读取数据
        DataSource<String> text = env.readTextFile("input/word.txt");
        //3、进行一系列转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = text.flatMap(new Tokenizer());
        AggregateOperator<Tuple2<String, Integer>> counts = wordAndOne
                .groupBy(0)
                .sum(1);
        //4、配置数据汇写出数据
        counts.print();

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