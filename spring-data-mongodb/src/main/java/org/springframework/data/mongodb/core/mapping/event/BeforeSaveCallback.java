/*
 * Copyright 2019-2023 the original author or authors.
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
import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Entity callback triggered before save of a document.
 *
 * @author Mark Paluch
 * @author Michael J. Simons
 * @since 2.2
 * @see org.springframework.data.mapping.callback.EntityCallbacks
 */
@FunctionalInterface
public interface BeforeSaveCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before a domain object is saved. Can return either the same or a modified instance
	 * of the domain object and can modify {@link Document} contents. This method is called after converting the
	 * {@code entity} to a {@link Document} so effectively the document is used as outcome of invoking this callback.
	 * Changes to the domain object are not taken into account for saving, only changes to the document. Only transient
	 * fields of the entity should be changed in this callback. To change persistent the entity before being converted,
	 * use the {@link BeforeConvertCallback}.
	 *
	 * @param entity the domain object to save.
	 * @param document {@link Document} representing the {@code entity}.
	 * @param collection name of the collection.
	 * @return the domain object to be persisted.
	 */
	T onBeforeSave(T entity, Document document, String collection);
}
