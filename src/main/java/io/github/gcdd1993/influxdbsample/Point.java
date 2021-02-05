package io.github.gcdd1993.influxdbsample;

import lombok.Data;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import java.time.Instant;

/**
 * @author gcdd1993
 * @date 2021/2/4
 * @since 1.0.0
 */
@Data
@Measurement(name = "point", database = "tabby1")
public class Point {
    @Column(name = "device_id", tag = true)
    private String deviceId;

    @Column(name = "key", tag = true)
    private String key;

    @Column(name = "value")
    private Double value;

    @Column(name = "time")
    private Instant time;
}
