package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class SynchronousTraceRequestContext extends TraceRequestContext {

	SynchronousTraceRequestContext(Tracer tracer) {
		super(context(tracer));
	}

	SynchronousTraceRequestContext withObservation(Observation value) {

		put(Observation.class, value);
		return this;
	}

	private static Map<Object, Object> context(Tracer tracer) {

		Map<Object, Object> map = new ConcurrentHashMap<>();

		Span currentSpan = tracer.currentSpan();

		if (currentSpan == null) {
			return map;
		}

		map.put(Span.class, currentSpan);
		map.put(TraceContext.class, currentSpan.context());

		return map;
	}
}
