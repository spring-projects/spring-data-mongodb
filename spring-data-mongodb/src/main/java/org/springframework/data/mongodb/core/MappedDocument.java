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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.StreamUtils;

import com.mongodb.client.model.Filters;

/**
 * A MongoDB document in its mapped state. I.e. after a source document has been mapped using mapping information of the
 * entity the source document was supposed to represent.
 * 
 * @author Oliver Gierke
 * @since 2.1
 */
@RequiredArgsConstructor(staticName = "of")
public class MappedDocument {

	private static final String ID_FIELD = "_id";
	private static final Document ID_ONLY_PROJECTION = new Document(ID_FIELD, 1);

	private final @Getter Document document;

	public static Document getIdOnlyProjection() {
		return ID_ONLY_PROJECTION;
	}

	public static Document getIdIn(Collection<?> ids) {
		return new Document(ID_FIELD, new Document("$in", ids));
	}

	public static List<Object> toIds(Collection<Document> documents) {

		return documents.stream()//
				.map(it -> it.get(ID_FIELD))//
				.collect(StreamUtils.toUnmodifiableList());
	}

	public boolean hasId() {
		return document.containsKey(ID_FIELD);
	}

	public boolean hasNonNullId() {
		return hasId() && document.get(ID_FIELD) != null;
	}

	public Object getId() {
		return document.get(ID_FIELD);
	}

	public <T> T getId(Class<T> type) {
		return document.get(ID_FIELD, type);
	}

	public boolean isIdPresent(Class<?> type) {
		return type.isInstance(getId());
	}

	public Bson getIdFilter() {
		return Filters.eq(ID_FIELD, document.get(ID_FIELD));
	}

	public Update updateWithoutId() {
		return Update.fromDocument(document, ID_FIELD);
	}
}
