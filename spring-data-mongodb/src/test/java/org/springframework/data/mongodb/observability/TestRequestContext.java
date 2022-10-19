package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TestRequestContext extends TraceRequestContext {

	static TestRequestContext withObservation(Observation value) {
		return new TestRequestContext(value);
	}

	private TestRequestContext(Observation value) {
		super(context(value));
	}

	private static Map<Object, Object> context(Observation value) {

		Map<Object, Object> map = new ConcurrentHashMap<>();

		map.put(Observation.class, value);

		return map;
	}
}
