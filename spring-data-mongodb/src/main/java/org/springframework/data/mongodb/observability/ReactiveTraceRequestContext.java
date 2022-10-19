package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;
import reactor.util.context.ContextView;

import java.util.Map;
import java.util.stream.Collectors;

class ReactiveTraceRequestContext extends TraceRequestContext {

	ReactiveTraceRequestContext withObservation(Observation value) {

		put(Observation.class, value);
		return this;
	}

	ReactiveTraceRequestContext(ContextView context) {
		super(context.stream().collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue)));
	}
}
