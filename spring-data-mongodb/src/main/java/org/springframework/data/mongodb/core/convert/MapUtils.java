/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import com.mongodb.DBObject;

/**
 * @author Mark Paluch
 */
class MapUtils {
	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 *
	 * @param source
	 * @return
	 */
	static Collection<?> asCollection(Object source) {

		if (source instanceof Collection) {
			return (Collection<?>) source;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	@SuppressWarnings("unchecked")
	static Map<String, Object> asMap(Bson bson) {

		if (bson instanceof Document) {
			return (Document) bson;
		}

		if (bson instanceof DBObject) {
			return ((DBObject) bson).toMap();
		}

		throw new IllegalArgumentException(
				String.format("Cannot read %s. as map. Given Bson must be a Document or DBObject!", bson.getClass()));
	}

	static void addToMap(Bson bson, String key, @Nullable Object value) {

		if (bson instanceof Document) {
			((Document) bson).put(key, value);
			return;
		}
		if (bson instanceof DBObject) {
			((DBObject) bson).put(key, value);
			return;
		}
		throw new IllegalArgumentException(String.format(
				"Cannot add key/value pair to %s. as map. Given Bson must be a Document or DBObject!", bson.getClass()));
	}

	static void addAllToMap(Bson bson, Map<String, ?> value) {

		if (bson instanceof Document) {
			((Document) bson).putAll(value);
			return;
		}

		if (bson instanceof DBObject) {
			((DBObject) bson).putAll(value);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot add all to %s. Given Bson must be a Document or DBObject.", bson.getClass()));
	}

	static void removeFromMap(Bson bson, String key) {

		if (bson instanceof Document) {
			((Document) bson).remove(key);
			return;
		}

		if (bson instanceof DBObject) {
			((DBObject) bson).removeField(key);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot remove from %s. Given Bson must be a Document or DBObject.", bson.getClass()));
	}
}
