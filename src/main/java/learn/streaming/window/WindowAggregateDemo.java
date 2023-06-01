package learn.streaming.window;

import learn.streaming.source.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Date;

/**
 * reduce的输出输入类型都要一样
 */
public class WindowAggregateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                });

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 1：输入数据的类型
        // 2：累加器类型
        // 3：输出的类型
        sensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法");
                        return accumulator + value.getVc();
                    }

                    /**
                     * 获取最终结果，窗口触发时输出
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("调用GetResult");
                        return accumulator.toString();
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                })
                .print();


        env.execute();
    }

}
