/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.util;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class BsonUtils {

	public static <T> T get(Bson bson, String key) {
		return (T) asMap(bson).get(key);
	}

	public static Map<String, Object> asMap(Bson bson) {
		if (bson instanceof Document) {
			return (Document) bson;
		}
		if (bson instanceof BasicDBObject) {
			return ((BasicDBObject) bson);
		}
		throw new IllegalArgumentException("o_O what's that? Cannot read values from " + bson.getClass());
	}

	public static void addToMap(Bson bson, String key, Object value) {

		if (bson instanceof Document) {
			((Document) bson).put(key, value);
			return;
		}
		if (bson instanceof DBObject) {
			((DBObject) bson).put(key, value);
			return;
		}
		throw new IllegalArgumentException("o_O what's that? Cannot add value to " + bson.getClass());
	}

	public static void addAllToMap(Bson bson, Map value) {

		if (bson instanceof Document) {
			((Document) bson).putAll((Map) value);
			return;
		}
		if (bson instanceof DBObject) {
			((DBObject) bson).putAll((Map) value);
			return;
		}
		throw new IllegalArgumentException("o_O what's that? Cannot add value to " + bson.getClass());
	}

	public static void removeFromMap(Bson bson, String key) {

		if (bson instanceof Document) {
			((Document) bson).remove(key);
			return;
		}
		if (bson instanceof DBObject) {
			((DBObject) bson).removeField(key);
			return;
		}
		throw new IllegalArgumentException("o_O what's that? Cannot add value to " + bson.getClass());
	}
}
