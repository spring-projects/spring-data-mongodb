/*
 * Copyright 2022-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import reactor.core.CoreSubscriber;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import org.springframework.data.util.ReactiveWrappers;
import org.springframework.data.util.ReactiveWrappers.ReactiveLibrary;
import org.springframework.util.ClassUtils;

import com.mongodb.ContextProvider;
import com.mongodb.RequestContext;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.reactivestreams.client.ReactiveContextProvider;

/**
 * Factory to create a {@link ContextProvider} to propagate the request context across tasks. Requires either
 * {@link SynchronousContextProvider} or {@link ReactiveContextProvider} to be present.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class ContextProviderFactory {

	private static final boolean SYNCHRONOUS_PRESENT = ClassUtils
			.isPresent("com.mongodb.client.SynchronousContextProvider", ContextProviderFactory.class.getClassLoader());

	private static final boolean REACTIVE_PRESENT = ClassUtils.isPresent(
			"com.mongodb.reactivestreams.client.ReactiveContextProvider", ContextProviderFactory.class.getClassLoader())
			&& ReactiveWrappers.isAvailable(ReactiveLibrary.PROJECT_REACTOR);

	/**
	 * Create a {@link ContextProvider} given {@link ObservationRegistry}. The factory method attempts to create a
	 * {@link ContextProvider} that is capable to propagate request contexts across imperative or reactive usage,
	 * depending on their class path presence.
	 *
	 * @param observationRegistry must not be {@literal null}.
	 * @return
	 */
	public static ContextProvider create(ObservationRegistry observationRegistry) {

		if (SYNCHRONOUS_PRESENT && REACTIVE_PRESENT) {
			return new CompositeContextProvider(observationRegistry);
		}

		if (SYNCHRONOUS_PRESENT) {
			return new DefaultSynchronousContextProvider(observationRegistry);
		}

		if (REACTIVE_PRESENT) {
			return DefaultReactiveContextProvider.INSTANCE;
		}

		throw new IllegalStateException(
				"Cannot create ContextProvider. Neither SynchronousContextProvider nor ReactiveContextProvider is on the class path.");
	}

	record DefaultSynchronousContextProvider(
			ObservationRegistry observationRegistry) implements SynchronousContextProvider {

		@Override
		public RequestContext getContext() {

			MapRequestContext requestContext = new MapRequestContext();

			Observation currentObservation = observationRegistry.getCurrentObservation();
			if (currentObservation != null) {
				requestContext.put(ObservationThreadLocalAccessor.KEY, currentObservation);
			}

			return requestContext;
		}

	}

	enum DefaultReactiveContextProvider implements ReactiveContextProvider {

		INSTANCE;

		@Override
		public RequestContext getContext(Subscriber<?> subscriber) {

			if (subscriber instanceof CoreSubscriber<?> cs) {

				Map<Object, Object> map = cs.currentContext().stream()
						.collect(Collectors.toConcurrentMap(Entry::getKey, Entry::getValue));

				return new MapRequestContext(map);
			}

			return new MapRequestContext();
		}
	}

	record CompositeContextProvider(DefaultSynchronousContextProvider synchronousContextProvider)
			implements
				SynchronousContextProvider,
				ReactiveContextProvider {

		CompositeContextProvider(ObservationRegistry observationRegistry) {
			this(new DefaultSynchronousContextProvider(observationRegistry));
		}

		@Override
		public RequestContext getContext() {
			return synchronousContextProvider.getContext();
		}

		@Override
		public RequestContext getContext(Subscriber<?> subscriber) {
			return DefaultReactiveContextProvider.INSTANCE.getContext(subscriber);
		}
	}

}
