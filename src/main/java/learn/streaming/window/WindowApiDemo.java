package learn.streaming.window;

import learn.streaming.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowApiDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(WaterSensor::getId);

        // 1.指定窗口分配器
        // 1.1 没有keyBy的窗口，原始stream不会分流，都在同一个sub task执行，并行度只能为1。
//        keyedStream.windowAll();

        // 1.2. 有keyBy的窗口
        // 基于时间
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));//滑动窗口，窗口长度10s，滑动步长2s
// keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动窗口，窗口长度10s，滑动步长2s
        // keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))); // 会话窗口，超时间隔5s

        // 基于次数
        // keyedStream.countWindow(5) // 滚动窗口，窗口长度=5个元素
        // keyedStream.countWindow(5, 2); // 滚动窗口，窗口长度=5个元素，滑动步长=2个元素
        // keyedStream.window(GlobalWindows.create()); // 全局窗口，计数窗口的底层就是用这个

        // 2.指定窗口函数：窗口内数据的计算逻辑
        // 增量聚合：来一条算一条
        // 全窗口函数：数据来了不计算，存起来，窗口触发的时候计算并输出结果

    }

}
