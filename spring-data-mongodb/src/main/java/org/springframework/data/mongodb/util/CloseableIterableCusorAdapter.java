/*
 * Copyright 2015 the original author or authors.
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

import java.util.Iterator;

import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.util.CloseableIterator;

import com.mongodb.Cursor;
import com.mongodb.DBObject;

/**
 * @author Thomas Darimont
 */
public class CloseableIterableCusorAdapter<T> implements CloseableIterator<T> {

	private volatile Cursor cursor;
	private MongoConverter converter;
	private Class<?> entityClass;

	public CloseableIterableCusorAdapter(Cursor cursor, MongoConverter converter, Class<?> entityClass) {

		this.cursor = cursor;
		this.converter = converter;
		this.entityClass = entityClass;
	}

	@Override
	public boolean hasNext() {

		if (cursor == null) {
			return false;
		}

		try {
			return cursor.hasNext();
		} catch (Exception ex) {
			throw new RuntimeException("error on hasNext");
		}
	}

	@Override
	public T next() {

		if (cursor == null) {
			return null;
		}

		try {

			DBObject item = cursor.next();
			Object converted = converter.read(entityClass, item);

			return (T) converted;
		} catch (Exception ex) {
			throw new RuntimeException("error on next");
		}
	}

	@Override
	public void close() {

		Cursor c = cursor;
		try {
			c.close();
		} finally {

			cursor = null;
			converter = null;
			entityClass = null;
		}
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}
}
