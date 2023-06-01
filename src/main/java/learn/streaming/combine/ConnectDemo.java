package learn.streaming.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> source2 = env.fromElements("111", "222", "333");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);
        env.execute();

    }
}
