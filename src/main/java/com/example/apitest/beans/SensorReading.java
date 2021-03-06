package com.example.apitest.beans;

/**
 * Desription: 传感器温度读书的数据类型
 *
 * @ClassName SensorReading
 * @Author Zhanyuwei
 * @Date 2020/12/14 9:56 下午
 * @Version 1.0
 **/
public class SensorReading {

    private String id;

    private Long timestamp;

    private Double temperature;

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public SensorReading() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
