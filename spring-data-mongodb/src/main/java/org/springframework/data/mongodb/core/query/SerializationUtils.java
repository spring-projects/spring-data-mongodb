/*
 * Copyright 2012-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

import com.mongodb.util.JSON;

/**
 * Utility methods for JSON serialization.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class SerializationUtils {

	private SerializationUtils() {

	}

	/**
	 * Flattens out a given {@link Document}.
	 *
	 * <pre>
	 * <code>
	 * {
	 *   _id : 1
	 *   nested : { value : "conflux"}
	 * }
	 * </code>
	 * will result in
	 * <code>
	 * {
	 *   _id : 1
	 *   nested.value : "conflux"
	 * }
	 * </code>
	 * </pre>
	 *
	 * @param source can be {@literal null}.
	 * @return {@link Collections#emptyMap()} when source is {@literal null}
	 * @since 1.8
	 */
	public static Map<String, Object> flattenMap(@Nullable Document source) {

		if (source == null) {
			return Collections.emptyMap();
		}

		Map<String, Object> result = new HashMap<String, Object>();
		toFlatMap("", source, result);
		return result;
	}

	private static void toFlatMap(String currentPath, Object source, Map<String, Object> map) {

		if (source instanceof Document) {

			Document document = (Document) source;
			Iterator<Map.Entry<String, Object>> iter = document.entrySet().iterator();
			String pathPrefix = currentPath.isEmpty() ? "" : currentPath + ".";

			while (iter.hasNext()) {

				Map.Entry<String, Object> entry = iter.next();

				if (entry.getKey().startsWith("$")) {
					if (map.containsKey(currentPath)) {
						((Document) map.get(currentPath)).put(entry.getKey(), entry.getValue());
					} else {
						map.put(currentPath, new Document(entry.getKey(), entry.getValue()));
					}
				} else {

					toFlatMap(pathPrefix + entry.getKey(), entry.getValue(), map);
				}
			}
		} else {
			map.put(currentPath, source);
		}
	}

	/**
	 * Serializes the given object into pseudo-JSON meaning it's trying to create a JSON representation as far as possible
	 * but falling back to the given object's {@link Object#toString()} method if it's not serializable. Useful for
	 * printing raw {@link Document}s containing complex values before actually converting them into Mongo native types.
	 *
	 * @param value
	 * @return the serialized value or {@literal null}.
	 */
	@Nullable
	public static String serializeToJsonSafely(@Nullable Object value) {

		if (value == null) {
			return null;
		}

		try {
			return value instanceof Document ? ((Document) value).toJson() : JSON.serialize(value);
		} catch (Exception e) {

			if (value instanceof Collection) {
				return toString((Collection<?>) value);
			} else if (value instanceof Map) {
				return toString((Map<?, ?>) value);
			} else {
				return String.format("{ \"$java\" : %s }", value.toString());
			}
		}
	}

	private static String toString(Map<?, ?> source) {
		return iterableToDelimitedString(source.entrySet(), "{ ", " }", new Converter<Entry<?, ?>, Object>() {
			public Object convert(Entry<?, ?> source) {
				return String.format("\"%s\" : %s", source.getKey(), serializeToJsonSafely(source.getValue()));
			}
		});
	}

	private static String toString(Collection<?> source) {
		return iterableToDelimitedString(source, "[ ", " ]", new Converter<Object, Object>() {
			public Object convert(Object source) {
				return serializeToJsonSafely(source);
			}
		});
	}

	/**
	 * Creates a string representation from the given {@link Iterable} prepending the postfix, applying the given
	 * {@link Converter} to each element before adding it to the result {@link String}, concatenating each element with
	 * {@literal ,} and applying the postfix.
	 *
	 * @param source
	 * @param prefix
	 * @param postfix
	 * @param transformer
	 * @return
	 */
	private static <T> String iterableToDelimitedString(Iterable<T> source, String prefix, String postfix,
			Converter<? super T, Object> transformer) {

		StringBuilder builder = new StringBuilder(prefix);
		Iterator<T> iterator = source.iterator();

		while (iterator.hasNext()) {
			builder.append(transformer.convert(iterator.next()));
			if (iterator.hasNext()) {
				builder.append(", ");
			}
		}

		return builder.append(postfix).toString();
	}
}
