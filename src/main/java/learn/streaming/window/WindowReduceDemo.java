package learn.streaming.window;

import learn.streaming.source.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
public class WindowReduceDemo {

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

        sensorWS.reduce(
                        new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                System.out.println("调用reduce方法，value1=" + value1 + ",value2=" + value2 + ",date=" + new Date());
                                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                            }
                        })
                .print();


        env.execute();
    }

}
