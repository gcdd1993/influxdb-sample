package io.github.gcdd1993.influxdbsample;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * @author gcdd1993
 * @date 2021/2/5
 * @since 1.0.0
 */
public class ReactiveInfluxWriter {

    Mono<Void> write(Publisher<Point> pointPublisher) {
        return Mono.empty();
    }

}
