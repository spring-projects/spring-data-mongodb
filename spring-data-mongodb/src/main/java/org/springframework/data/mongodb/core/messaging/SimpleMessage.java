/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Trivial {@link Message} implementation.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
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

		Assert.notNull(properties, "Properties must not be null Use MessageProperties.empty() instead");

		this.raw = raw;
		this.body = body;
		this.properties = properties;
	}

	@Override
	public S getRaw() {
		return raw;
	}

	@Override
	public T getBody() {
		return body;
	}

	@Override
	public MessageProperties getProperties() {
		return properties;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SimpleMessage<?, ?> that = (SimpleMessage<?, ?>) o;

		if (!ObjectUtils.nullSafeEquals(this.raw, that.raw)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.body, that.body)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(this.properties, that.properties);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(raw);
		result = 31 * result + ObjectUtils.nullSafeHashCode(body);
		result = 31 * result + ObjectUtils.nullSafeHashCode(properties);
		return result;
	}

	public String toString() {
		return "SimpleMessage(raw=" + this.getRaw() + ", body=" + this.getBody() + ", properties=" + this.getProperties()
				+ ")";
	}
}
