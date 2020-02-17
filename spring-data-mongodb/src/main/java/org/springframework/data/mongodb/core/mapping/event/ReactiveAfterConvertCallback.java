/*
 * Copyright 2020 the original author or authors.
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
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;

/**
 * Callback being invoked after a domain object is converted from a Document (when reading from the DB).
 *
 * @author Mark Paluch
 * @author Roman Puchkovskiy
 * @since 3.0
 * @see ReactiveEntityCallbacks
 */
@FunctionalInterface
public interface ReactiveAfterConvertCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked after a domain object is converted from a Document. Can return either the same
	 * or a modified instance of the domain object.
	 *
	 * @param entity the domain object (the result of the conversion).
	 * @param document must not be {@literal null}.
	 * @param collection name of the collection.
	 * @return a {@link Publisher} emitting the domain object that is the result of the conversion from the Document.
	 */
	Publisher<T> onAfterConvert(T entity, Document document, String collection);
}
