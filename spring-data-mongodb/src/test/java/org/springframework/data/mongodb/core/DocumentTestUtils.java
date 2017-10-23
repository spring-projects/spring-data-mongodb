/*
 * Copyright 2012-2016 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBList;

/**
 * Helper classes to ease assertions on {@link Document}s.
 * 
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public abstract class DocumentTestUtils {

	private DocumentTestUtils() {}

	/**
	 * Expects the field with the given key to be not {@literal null} and a {@link Document} in turn and returns it.
	 * 
	 * @param source the {@link Document} to lookup the nested one
	 * @param key the key of the field to lokup the nested {@link Document}
	 * @return
	 */
	public static Document getAsDocument(Document source, String key) {
		return getTypedValue(source, key, Document.class);
	}

	/**
	 * Expects the field with the given key to be not {@literal null} and a {@link BasicDBList}.
	 * 
	 * @param source the {@link Document} to lookup the {@link List} in
	 * @param key the key of the field to find the {@link List} in
	 * @return
	 */
	public static <T> List<T> getAsDBList(Document source, String key) {
		return getTypedValue(source, key, List.class);
	}

	/**
	 * Expects the list element with the given index to be a non-{@literal null} {@link Document} and returns it.
	 * 
	 * @param source the {@link List} to look up the {@link Document} element in
	 * @param index the index of the element expected to contain a {@link Document}
	 * @return
	 */
	public static Document getAsDocument(List<?> source, int index) {

		assertThat(source.size()).isGreaterThanOrEqualTo(index + 1);
		Object value = source.get(index);
		assertThat(value).isInstanceOf(Document.class);
		return (Document) value;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getTypedValue(Document source, String key, Class<T> type) {

		Object value = source.get(key);
		assertThat(value).isNotNull();
		assertThat(value).isInstanceOf(type);

		return (T) value;
	}

	public static void assertTypeHint(Document document, Class<?> type) {
		assertTypeHint(document, type.getName());
	}

	public static void assertTypeHint(Document document, String expectedTypeString) {

		Iterator<String> keyIterator = document.keySet().iterator();
		while (keyIterator.hasNext()) {
			String key = keyIterator.next();
			if (key.equals("_class")) {
				assertThat(document.get(key)).isEqualTo(expectedTypeString);
				assertThat(keyIterator.hasNext()).isFalse();
				return;
			}
		}

		fail(String.format("Expected to find type info %s in %s.", document, expectedTypeString));
	}
}
