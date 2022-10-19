package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

import com.mongodb.reactivestreams.client.ReactiveContextProvider;

/**
 * Helper functions to ease registration of Spring Data MongoDB's observability.
 */
public class MongoMetricsReactiveConfigurationHelper {

	public static ReactiveContextProvider reactiveContextProvider(ObservationRegistry registry) {
		return subscriber -> {
			if (subscriber instanceof CoreSubscriber<?> coreSubscriber) {
				return new ReactiveTraceRequestContext(coreSubscriber.currentContext())
						.withObservation(Observation.start("name", registry));
			}
			return new ReactiveTraceRequestContext(Context.empty()).withObservation(Observation.start("name", registry));
		};
	}
}
