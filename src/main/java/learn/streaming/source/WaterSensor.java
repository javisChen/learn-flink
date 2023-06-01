package learn.streaming.source;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class WaterSensor {

    private String id;
    private Long ts;
    private Integer vc;
}
