import io.github.gcdd1993.influxdbsample.Point;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.impl.InfluxDBMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * https://docs.influxdata.com/influxdb/v2.0/write-data/best-practices/
 *
 * @author gcdd1993
 * @date 2021/2/4
 * @since 1.0.0
 */
@Slf4j
public class DataWriteTest {

    @Test
    void _writeOnce() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby1");
        var point = new Point();
        point.setKey("111");
        point.setValue(1.0);
        point.setDeviceId("448353124370571");
        point.setTime(Instant.now());
        var mapper = new InfluxDBMapper(client);
        mapper.save(point);
        client.close();
    }

    @Test
    void _writeOnce1() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby1");
        client.write(
                org.influxdb.dto.Point
                        .measurement("device.point")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("device_id", "540615790879219")
                        .tag("point_key", "111")
                        .addField("value", 1.01)
                        .build()
        );
        client.close();
    }

    @Test
    void _writeBatch() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby1");
        var batchPointsBuilder = BatchPoints
                .database("tabby1")
                .retentionPolicy("autogen");
        var startTime = System.currentTimeMillis();
//        var batchSize = 1000; 550ms
        var batchSize = 5000; // 550ms
//        var batchSize = 10_000; 774ms
//        var batchSize = 100_000; // 2319ms
//        var batchSize = 1000_000; 失败
        for (int i = 0; i < batchSize; i++) {
            var point = org.influxdb.dto.Point
                    .measurement("device.point")
                    .time(System.currentTimeMillis() - 100 * i, TimeUnit.MILLISECONDS)
                    .tag("device_id", "540615790879219")
                    .tag("point_key", "112")
                    .addField("value", ThreadLocalRandom.current().nextDouble(0, 10))
                    .build();
            batchPointsBuilder.point(point);
        }
        client.write(batchPointsBuilder.build());
        client.close();
        var endTime = System.currentTimeMillis();

        log.info("耗时: {}ms.", (endTime - startTime));
    }

    /**
     * 自动批处理
     */
    @Test
    void _writeBatch1() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setRetentionPolicy("autogen");
        client.setConsistency(InfluxDB.ConsistencyLevel.ONE);
        client.setDatabase("tabby1");
        client.enableBatch(10000, 1000, TimeUnit.MILLISECONDS);
//        var batchSize = 1000; 550ms
//        var batchSize = 10_000; 774ms
        var batchSize = 100_000; // 44398ms
//        var batchSize = 1000_000; 失败
        // write
        var startTime = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            var point = org.influxdb.dto.Point
                    .measurement("device.point")
                    .time(System.currentTimeMillis() - 100 * i, TimeUnit.MILLISECONDS)
                    .tag("device_id", "540615790879219")
                    .tag("point_key", "113")
                    .addField("value", ThreadLocalRandom.current().nextDouble(0, 10))
                    .build();
            client.write(point);
        }
        client.close();
        var endTime = System.currentTimeMillis();

        log.info("耗时: {}ms.", (endTime - startTime));
    }

    /**
     * 批处理，使用GZip
     */
    @Test
    void _writeBatch2() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby1");
        client.enableGzip();
        Assertions.assertTrue(client.isGzipEnabled());
        var batchPointsBuilder = BatchPoints
                .database("tabby1")
                .retentionPolicy("autogen");
        var startTime = System.currentTimeMillis();
//        var batchSize = 1000; 550ms
        var batchSize = 5000; // 550ms
//        var batchSize = 10_000; 774ms
//        var batchSize = 100_000; // 2319ms
//        var batchSize = 1000_000; 失败
        for (int i = 0; i < batchSize; i++) {
            var point = org.influxdb.dto.Point
                    .measurement("device.point")
                    .time(System.currentTimeMillis() - 100 * i, TimeUnit.MILLISECONDS)
                    .tag("device_id", "540615790879219")
                    .tag("point_key", "112")
                    .addField("value", ThreadLocalRandom.current().nextDouble(0, 10))
                    .build();
            batchPointsBuilder.point(point);
        }
        client.write(batchPointsBuilder.build());
        client.close();
        var endTime = System.currentTimeMillis();

        log.info("耗时: {}ms.", (endTime - startTime));
    }


    /**
     * 自动批处理 & 写入失败重试
     */
    @Test
    @Deprecated
    void _writeBatchRetry1() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setRetentionPolicy("autogen");
        client.setConsistency(InfluxDB.ConsistencyLevel.ONE);
        client.setDatabase("tabby1");
        client.enableGzip(); // 3194ms -> 2728ms

        var options = BatchOptions
                .DEFAULTS
                .bufferLimit(6_000)
                .actions(5_000)
                .consistency(InfluxDB.ConsistencyLevel.ANY)
                .flushDuration(1000)
                .jitterDuration(100);
        client.enableBatch(options);
//        var batchSize = 1000; 550ms
//        var batchSize = 10_000; 774ms
        var batchSize = 100_000; // 耗时: 3194ms.
//        var batchSize = 1000_000; 失败
        // write
        var startTime = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            var point = org.influxdb.dto.Point
                    .measurement("device.point")
                    .time(System.currentTimeMillis() - 100 * i, TimeUnit.MILLISECONDS)
                    .tag("device_id", "540615790879219")
                    .tag("point_key", "114")
                    .addField("value", ThreadLocalRandom.current().nextDouble(0, 10))
                    .build();
            client.write(point);
        }
        client.close();
        var endTime = System.currentTimeMillis();

        log.info("耗时: {}ms.", (endTime - startTime));

        for (; ; ) {
            // blocking
        }
    }

}
