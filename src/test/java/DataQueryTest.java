import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @author gcdd1993
 * @date 2021/2/4
 * @since 1.0.0
 */
@Slf4j
public class DataQueryTest {

    /**
     * 聚合查询一个盒子下面所有点每个整点的增量和
     */
    @Test
    void _query01() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby");
        client.query(new Query("SELECT SUM(\"incresement\") FROM (SELECT LAST(\"value\") - FIRST(\"value\") AS \"incresement\" FROM \"point\" WHERE \"client_id\" = '996240772645303'  GROUP BY time(1h),\"point_key\") GROUP BY time(1h)"),
                res -> {
                    var results = res.getResults();
                    log.info("get results: {}", results.size());
                    results
                            .forEach(result -> {
                                var series = result.getSeries();
                                series
                                        .forEach(s -> log.info("series: {}", s));
                            });
                }, ex -> {
                    log.error("error ", ex);
                });
//        var mapper = new InfluxDBMapper(client);
//        mapper.query(new Query(""))
        for (; ; ) {
            // blocking
        }
    }

    @Test
    void _query02() {
        var client = InfluxDBFactory.connect(System.getenv("TSDB_URL"), "admin", "1123LOVEwm@");
        client.setDatabase("tabby");
        var query = new Query("SELECT SUM(\"incresement\") FROM (SELECT LAST(\"value\") - FIRST(\"value\") AS \"incresement\" FROM \"point\" WHERE \"client_id\" = '996240772645303'  GROUP BY time(1h),\"point_key\") GROUP BY time(1h)");
        var result = client.query(query, TimeUnit.MILLISECONDS);
        result.getResults()
                .stream()
                .map(QueryResult.Result::getSeries)
                .flatMap(Collection::stream)
                .map(QueryResult.Series::getValues)
                .flatMap(Collection::stream)
                .forEach(it -> {
                    var ts = Instant.ofEpochMilli((Long.parseLong(String.valueOf(it.get(0)))));
                    var value = (Double) it.get(1);
                    log.info("value: {}, ts: {}", value, ts);
                });
    }

}
