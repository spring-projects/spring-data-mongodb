/*
 * Copyright 2012 - 2014 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Helper classes to ease assertions on {@link DBObject}s.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public abstract class DBObjectTestUtils {

	private DBObjectTestUtils() {

	}

	/**
	 * Extracts value for a given path within the dbo. Indexes in arrays can be addressed via {@code []}.
	 * 
	 * @param source
	 * @param path
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getValue(DBObject source, String path) {

		String[] fragments = path.split("\\.");
		if (fragments.length == 1) {
			return (T) source.get(path);
		}

		Iterator<String> it = Arrays.asList(fragments).iterator();

		DBObject dbo = source;
		while (it.hasNext()) {

			String key = it.next();

			if (key.startsWith("[")) {
				String indexNumber = key.substring(1, key.indexOf("]"));
				dbo = getAsDBObject((BasicDBList) dbo, Integer.parseInt(indexNumber));
			} else {

				if (!it.hasNext()) {
					return (T) dbo.get(key);
				}

				Object value = dbo.get(key);
				if (value instanceof DBObject) {
					dbo = (DBObject) value;
				} else {
					if (it.next().startsWith("$")) {
						return (T) value;
					}
				}
			}
		}

		throw new NoSuchElementException(String.format("Unable to find '%s' in %s.", path, source));
	}

	/**
	 * Expects the field with the given key to be not {@literal null} and a {@link DBObject} in turn and returns it.
	 * 
	 * @param source the {@link DBObject} to lookup the nested one
	 * @param key the key of the field to lokup the nested {@link DBObject}
	 * @return
	 */
	public static DBObject getAsDBObject(DBObject source, String key) {
		return getTypedValue(source, key, DBObject.class);
	}

	/**
	 * Expects the field with the given key to be not {@literal null} and a {@link BasicDBList}.
	 * 
	 * @param source the {@link DBObject} to lookup the {@link BasicDBList} in
	 * @param key the key of the field to find the {@link BasicDBList} in
	 * @return
	 */
	public static BasicDBList getAsDBList(DBObject source, String key) {
		return getTypedValue(source, key, BasicDBList.class);
	}

	/**
	 * Expects the list element with the given index to be a non-{@literal null} {@link DBObject} and returns it.
	 * 
	 * @param source the {@link BasicDBList} to look up the {@link DBObject} element in
	 * @param index the index of the element expected to contain a {@link DBObject}
	 * @return
	 */
	public static DBObject getAsDBObject(BasicDBList source, int index) {

		assertThat(source.size(), greaterThanOrEqualTo(index + 1));
		Object value = source.get(index);
		assertThat(value, is(instanceOf(DBObject.class)));
		return (DBObject) value;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getTypedValue(DBObject source, String key, Class<T> type) {

		Object value = source.get(key);
		assertThat(value, is(notNullValue()));
		assertThat(value, is(instanceOf(type)));

		return (T) value;
	}
}
