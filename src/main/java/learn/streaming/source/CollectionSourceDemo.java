package learn.streaming.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6))
//                .print()
//                .setParallelism(1);
//
        env.fromElements(1, 2, 3, 4, 5, 6)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
