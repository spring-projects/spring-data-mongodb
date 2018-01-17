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
package org.springframework.data.mongodb.core;

import lombok.EqualsAndHashCode;

import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ClassUtils;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * {@link Message} implementation specific to MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change
 * Streams</a>.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@EqualsAndHashCode
public class ChangeStreamEvent<T> implements Message<ChangeStreamDocument<Document>, T> {

	private final ChangeStreamDocument<Document> raw;
	private final MessageProperties messageProperties;

	private final Class<T> targetType;
	private final MongoConverter converter;
	private final AtomicReference<T> converted = new AtomicReference<>();

	/**
	 * @param raw must not be {@literal null}.
	 * @param messageProperties must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	ChangeStreamEvent(ChangeStreamDocument<Document> raw, MessageProperties messageProperties, Class<T> targetType,
			MongoConverter converter) {

		this.raw = raw;
		this.messageProperties = messageProperties;
		this.targetType = targetType;
		this.converter = converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.Message#getRaw()
	 */
	@Override
	public ChangeStreamDocument<Document> getRaw() {
		return raw;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.Message#getBody()
	 */
	@Override
	public T getBody() {

		if (raw == null) {
			return targetType.cast(raw);
		}

		if (raw.getFullDocument() == null) {
			return targetType.cast(raw.getFullDocument());
		}

		return getConverted();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.Message#getMessageProperties()
	 */
	@Override
	public MessageProperties getProperties() {
		return messageProperties;
	}

	private T getConverted() {

		T result = converted.get();
		if (result != null) {
			return result;
		}

		if (ClassUtils.isAssignable(Document.class, raw.getFullDocument().getClass())) {

			result = converter.read(targetType, raw.getFullDocument());
			return converted.compareAndSet(null, result) ? result : converted.get();
		}

		if (converter.getConversionService().canConvert(raw.getFullDocument().getClass(), targetType)) {

			result = converter.getConversionService().convert(raw.getFullDocument(), targetType);
			return converted.compareAndSet(null, result) ? result : converted.get();
		}

		throw new IllegalArgumentException(String.format("No converter found capable of converting %s to %s",
				raw.getFullDocument().getClass(), targetType));
	}

	@Override
	public String toString() {
		return "ChangeStreamEvent {" + "raw=" + raw + ", targetType=" + targetType + ", messageProperties="
				+ messageProperties + '}';
	}
}
