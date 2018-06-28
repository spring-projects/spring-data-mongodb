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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

/**
 * {@link Message} implementation specific to MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change
 * Streams</a>.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
@EqualsAndHashCode
public class ChangeStreamEvent<T> {

	@SuppressWarnings("rawtypes") //
	private static final AtomicReferenceFieldUpdater<ChangeStreamEvent, Object> CONVERTED_UPDATER = AtomicReferenceFieldUpdater
			.newUpdater(ChangeStreamEvent.class, Object.class, "converted");

	private final @Nullable ChangeStreamDocument<Document> raw;

	private final Class<T> targetType;
	private final MongoConverter converter;

	// accessed through CONVERTED_UPDATER.
	private volatile @Nullable T converted;

	/**
	 * @param raw can be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public ChangeStreamEvent(@Nullable ChangeStreamDocument<Document> raw, Class<T> targetType,
			MongoConverter converter) {

		this.raw = raw;
		this.targetType = targetType;
		this.converter = converter;
	}

	/**
	 * Get the raw {@link ChangeStreamDocument} as emitted by the driver.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public ChangeStreamDocument<Document> getRaw() {
		return raw;
	}

	/**
	 * Get the {@link ChangeStreamDocument#getClusterTime() cluster time} as {@link Instant} the event was emitted at.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Instant getTimestamp() {
		return raw != null && raw.getClusterTime() != null ? Instant.ofEpochMilli(raw.getClusterTime().getValue()) : null;
	}

	/**
	 * Get the {@link ChangeStreamDocument#getResumeToken() resume token} for this event.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public BsonValue getResumeToken() {
		return raw != null ? raw.getResumeToken() : null;
	}

	/**
	 * Get the {@link ChangeStreamDocument#getOperationType() operation type} for this event.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public OperationType getOperationType() {
		return raw != null ? raw.getOperationType() : null;
	}

	/**
	 * Get the database name the event was originated at.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public String getDatabaseName() {
		return raw != null ? raw.getNamespace().getDatabaseName() : null;
	}

	/**
	 * Get the collection name the event was originated at.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public String getCollectionName() {
		return raw != null ? raw.getNamespace().getCollectionName() : null;
	}

	/**
	 * Get the potentially converted {@link ChangeStreamDocument#getFullDocument()}.
	 *
	 * @return {@literal null} when {@link #getRaw()} or {@link ChangeStreamDocument#getFullDocument()} is
	 *         {@literal null}.
	 */
	@Nullable
	public T getBody() {

		if (raw == null) {
			return null;
		}

		Document fullDocument = raw.getFullDocument();

		if (fullDocument == null) {
			return targetType.cast(fullDocument);
		}

		return getConverted(fullDocument);
	}

	@SuppressWarnings("unchecked")
	private T getConverted(Document fullDocument) {
		return (T) doGetConverted(fullDocument);
	}

	private Object doGetConverted(Document fullDocument) {

		Object result = CONVERTED_UPDATER.get(this);

		if (result != null) {
			return result;
		}

		if (ClassUtils.isAssignable(Document.class, fullDocument.getClass())) {

			result = converter.read(targetType, fullDocument);
			return CONVERTED_UPDATER.compareAndSet(this, null, result) ? result : CONVERTED_UPDATER.get(this);
		}

		if (converter.getConversionService().canConvert(fullDocument.getClass(), targetType)) {

			result = converter.getConversionService().convert(fullDocument, targetType);
			return CONVERTED_UPDATER.compareAndSet(this, null, result) ? result : CONVERTED_UPDATER.get(this);
		}

		throw new IllegalArgumentException(String.format("No converter found capable of converting %s to %s",
				fullDocument.getClass(), targetType));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ChangeStreamEvent {" + "raw=" + raw + ", targetType=" + targetType + '}';
	}
}
