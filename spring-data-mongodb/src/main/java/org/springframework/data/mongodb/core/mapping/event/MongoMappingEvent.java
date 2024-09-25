/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import java.util.function.Function;

import org.bson.Document;
import org.springframework.context.ApplicationEvent;
import org.springframework.lang.Nullable;

/**
 * Base {@link ApplicationEvent} triggered by Spring Data MongoDB.
 *
 * @author Jon Brisbin
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoMappingEvent<T> extends ApplicationEvent {

	private static final long serialVersionUID = 1L;
	private final @Nullable Document document;
	private final @Nullable String collectionName;

	/**
	 * Creates new {@link MongoMappingEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param document can be {@literal null}.
	 * @param collectionName can be {@literal null}.
	 */
	public MongoMappingEvent(T source, @Nullable Document document, @Nullable String collectionName) {

		super(source);
		this.document = document;
		this.collectionName = collectionName;
	}

	/**
	 * @return {@literal null} if not set.
	 */
	public @Nullable Document getDocument() {
		return document;
	}

	/**
	 * Get the collection the event refers to.
	 *
	 * @return {@literal null} if not set.
	 * @since 1.8
	 */
	public @Nullable String getCollectionName() {
		return collectionName;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public T getSource() {
		return (T) super.getSource();
	}

	/**
	 * Allows client code to change the underlying source instance by applying the given {@link Function}.
	 * 
	 * @param mapper the {@link Function} to apply, will only be applied if the source is not {@literal null}.
	 * @since 2.1
	 */
	final void mapSource(Function<T, T> mapper) {

		if (source == null) {
			return;
		}

		this.source = mapper.apply(getSource());
	}
}
