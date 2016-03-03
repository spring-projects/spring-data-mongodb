/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Reactive {@link org.springframework.data.repository.query.ParametersParameterAccessor} implementation that subscribes
 * to reactive parameter wrapper types upon creation. This class performs synchronization when acessing parameters.
 * 
 * @author Mark Paluch
 */
class ReactiveMongoParameterAccessor extends MongoParametersParameterAccessor {

	private final Object[] values;
	private final MonoProcessor<?>[] subscriptions;

	public ReactiveMongoParameterAccessor(MongoQueryMethod method, Object[] values) {

		super(method, values);

		this.values = values;
		this.subscriptions = new MonoProcessor<?>[values.length];

		for (int i = 0; i < values.length; i++) {

			Object value = values[i];

			if (value == null) {
				continue;
			}

			if (!ReactiveWrappers.supports(value.getClass())) {
				continue;
			}

			if (ReactiveWrappers.isSingleValueType(value.getClass())) {
				subscriptions[i] = ReactiveWrapperConverters.toWrapper(value, Mono.class).subscribe();
			} else {
				subscriptions[i] = ReactiveWrapperConverters.toWrapper(value, Flux.class).collectList().subscribe();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getValue(int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getValue(int index) {

		if (subscriptions[index] != null) {
			return (T) subscriptions[index].block();
		}

		return super.getValue(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.MongoParametersParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {

		Object[] result = new Object[values.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = getValue(i);
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getBindableValue(int)
	 */
	public Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}
}
