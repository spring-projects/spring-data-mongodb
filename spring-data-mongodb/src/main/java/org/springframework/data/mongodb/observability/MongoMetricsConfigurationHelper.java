package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;

import com.mongodb.client.SynchronousContextProvider;

/**
 * Helper functions to ease registration of Spring Data MongoDB's observability.
 */
public class MongoMetricsConfigurationHelper {

	public static SynchronousContextProvider synchronousContextProvider(Tracer tracer, ObservationRegistry registry) {
		return () -> new SynchronousTraceRequestContext(tracer).withObservation(Observation.start("name", registry));
	}

	public static void addObservationHandler(ObservationRegistry registry, Tracer tracer) {
		registry.observationConfig().observationHandler(new MongoTracingObservationHandler(tracer));
	}
}
