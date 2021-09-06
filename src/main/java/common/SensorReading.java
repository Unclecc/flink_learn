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

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
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
