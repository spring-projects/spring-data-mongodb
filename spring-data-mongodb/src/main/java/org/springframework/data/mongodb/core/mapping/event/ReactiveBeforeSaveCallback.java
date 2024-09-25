/*
 * Copyright 2019-2024 the original author or authors.
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

import org.bson.Document;
import org.reactivestreams.Publisher;

import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Entity callback triggered before save of a document.
 *
 * @author Mark Paluch
 * @since 2.2
 * @see org.springframework.data.mapping.callback.ReactiveEntityCallbacks
 */
@FunctionalInterface
public interface ReactiveBeforeSaveCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before a domain object is saved. Can return either the same or a modified instance
	 * of the domain object and can modify {@link Document} contents. This method is called after converting the
	 * {@code entity} to {@link Document} so effectively the document is used as outcome of invoking this callback.
	 *
	 * @param entity the domain object to save.
	 * @param document {@link Document} representing the {@code entity}.
	 * @param collection name of the collection.
	 * @return a {@link Publisher} emitting the domain object to be persisted.
	 */
	Publisher<T> onBeforeSave(T entity, Document document, String collection);
}
