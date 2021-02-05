import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 按照InfluxDB行协议造测试数据
 *
 * @author gcdd1993
 * @date 2021/2/4
 * @since 1.0.0
 */
public class DataGeneratorTest {

    private final Random random = new Random();

    @Test
    void gen01() throws IOException {
        var start = LocalDateTime.of(2021, 2, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant();
        var end = LocalDateTime.of(2021, 2, 3, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant();
        var duration = Duration.ofSeconds(1L);
        var clientId = "996240772645303";
        var pointKey = 1000;
        for (var i = 1; i < 11; i++) {
            pointKey = 1000 + i;
            var writer = Files.newBufferedWriter(Paths.get("point_" + pointKey + ".txt"), StandardCharsets.UTF_8);
            _generate02(clientId, String.valueOf(pointKey), start, end, duration, writer);
        }
    }

    /**
     * 数据点数据(波动量)
     * <p>
     * point,client_id=996240772645303 point_key=111 1566000000
     */
    private void _generate01(String clientId,
                             String pointKey,
                             Instant start,
                             Instant end,
                             Duration duration,
                             BufferedWriter writer) {
        var cqTemplate = "point,client_id=" + clientId + ",point_key=" + pointKey + " value={0} {1}\n";
        var standardTime = start;
        try {
            writer.append("# DDL\n")
                    .append("CREATE DATABASE tabby\n")
                    .append("# DML\n")
                    .append("# CONTEXT-DATABASE: tabby\n\n");
            while (standardTime.isBefore(end)) {
                var ts = standardTime.toEpochMilli();
                var value = ThreadLocalRandom.current().nextDouble(1, 10);
                writer.append(MessageFormat.format(cqTemplate, String.valueOf(value), String.valueOf(ts)));
                standardTime = standardTime.plusMillis(duration.toMillis());
            }
        } catch (IOException ex) {
            //
        } finally {
            try {
                writer.flush();
                writer.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    /**
     * 增量
     */
    private void _generate02(String clientId,
                             String pointKey,
                             Instant start,
                             Instant end,
                             Duration duration,
                             BufferedWriter writer) {
        var cqTemplate = "point,client_id=" + clientId + ",point_key=" + pointKey + " value={0} {1}\n";
        var standardTime = start;
        var value = 1.0;
        try {
            writer.append("# DDL\n")
                    .append("CREATE DATABASE tabby\n")
                    .append("# DML\n")
                    .append("# CONTEXT-DATABASE: tabby\n\n");
            while (standardTime.isBefore(end)) {
                var ts = standardTime.toEpochMilli();
                value = value + 0.1;
                writer.append(MessageFormat.format(cqTemplate, String.valueOf(value), String.valueOf(ts)));
                standardTime = standardTime.plusMillis(duration.toMillis());
            }
        } catch (IOException ex) {
            //
        } finally {
            try {
                writer.flush();
                writer.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

}
