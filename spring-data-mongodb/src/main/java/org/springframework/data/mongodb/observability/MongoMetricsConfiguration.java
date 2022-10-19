package org.springframework.data.mongodb.observability;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import org.springframework.context.annotation.Bean;

/**
 * Class to configure needed beans for MongoDB + Micrometer.
 */
public class MongoMetricsConfiguration {

    @Bean
    MongoObservationCommandListener mongoObservationCommandListener(ObservationRegistry registry) {
        return new MongoObservationCommandListener(registry);
    }

    @Bean
    MongoTracingObservationHandler mongoTracingObservationHandler(Tracer tracer) {
        return new MongoTracingObservationHandler(tracer);
    }
}
