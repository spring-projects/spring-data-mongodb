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

import lombok.ToString;

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
@ToString(of = { "delegate", "targetType" })
class LazyMappingDelegatingMessage<S, T> implements Message<S, T> {

	private final Message<S, ?> delegate;
	private final Class<T> targetType;
	private final MongoConverter converter;

	LazyMappingDelegatingMessage(Message<S, ?> delegate, Class<T> targetType, MongoConverter converter) {

		this.delegate = delegate;
		this.targetType = targetType;
		this.converter = converter;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getRaw()
	 */
	@Override
	public S getRaw() {
		return delegate.getRaw();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getBody()
	 */
	@Override
	public T getBody() {

		if (delegate.getBody() == null || targetType.equals(delegate.getBody().getClass())) {
			return targetType.cast(delegate.getBody());
		}

		Object messageBody = delegate.getBody();

		if (ClassUtils.isAssignable(Document.class, messageBody.getClass())) {
			return converter.read(targetType, (Document) messageBody);
		}

		if (converter.getConversionService().canConvert(messageBody.getClass(), targetType)) {
			return converter.getConversionService().convert(messageBody, targetType);
		}

		throw new IllegalArgumentException(
				String.format("No converter found capable of converting %s to %s", messageBody.getClass(), targetType));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.messaging.Message#getProperties()
	 */
	@Override
	public MessageProperties getProperties() {
		return delegate.getProperties();
	}
}
