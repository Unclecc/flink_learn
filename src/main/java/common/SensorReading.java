package common;

import lombok.Data;

/**
 * @author chenwen
 */
public class SensorReading {

    private Integer id;
    private Long timestamp;
    private Double temperature;

    public SensorReading(Integer id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
