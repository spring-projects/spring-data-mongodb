/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Trivial {@link Message} implementation.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@EqualsAndHashCode
@ToString
class SimpleMessage<S, T> implements Message<S, T> {

	private @Nullable final S raw;
	private @Nullable final T body;
	private final MessageProperties properties;

	/**
	 * @param raw
	 * @param body
	 * @param properties must not be {@literal null}. Use {@link MessageProperties#empty()} instead.
	 */
	SimpleMessage(@Nullable S raw, @Nullable T body, MessageProperties properties) {

		Assert.notNull(properties, "Properties must not be null! Use MessageProperties.empty() instead.");

		this.raw = raw;
		this.body = body;
		this.properties = properties;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getRaw()
	 */
	@Override
	public S getRaw() {
		return raw;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getBody()
	 */
	@Override
	public T getBody() {
		return body;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getProperties()
	 */
	@Override
	public MessageProperties getProperties() {
		return properties;
	}
}
