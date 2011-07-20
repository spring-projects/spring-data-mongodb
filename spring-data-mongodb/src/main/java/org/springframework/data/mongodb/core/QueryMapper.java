/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

/**
 * A helper class to encapsulate any modifications of a Query object before it gets submitted to the database.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class QueryMapper {

	private final MongoConverter converter;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link MongoConverter}.
	 *
	 * @param converter
	 */
	public QueryMapper(MongoConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
	}

	/**
	 * Replaces the property keys used in the given {@link DBObject} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata.
	 *
	 * @param query
	 * @param entity
	 * @return
	 */
	public DBObject getMappedObject(DBObject query, MongoPersistentEntity<?> entity) {
		String idKey = null;
		if (null != entity && entity.getIdProperty() != null) {
			idKey = entity.getIdProperty().getName();
		} else if (query.containsField("id")) {
			idKey = "id";
		} else if (query.containsField("_id")) {
			idKey = "_id";
		}

		DBObject newDbo = new BasicDBObject();
		for (String key : query.keySet()) {
			String newKey = key;
			Object value = query.get(key);
			if (key.equals(idKey)) {
				if (value instanceof DBObject) {
					DBObject valueDbo = (DBObject) value;
					if (valueDbo.containsField("$in") || valueDbo.containsField("$nin")) {
						String inKey = valueDbo.containsField("$in") ? "$in" : "$nin";
						List<Object> ids = new ArrayList<Object>();
						for (Object id : (Object[]) valueDbo.get(inKey)) {
							if (null != converter && !(id instanceof ObjectId)) {
								try {
									ObjectId oid = converter.convertObjectId(id);
									ids.add(oid);
								} catch (ConversionException ignored) {
									ids.add(id);
								}
							} else {
								ids.add(id);
							}
						}
						valueDbo.put(inKey, ids.toArray(new Object[ids.size()]));
					} else {
						value = getMappedObject((DBObject) value, entity);
					}
				} else if (null != converter) {
					try {
						value = converter.convertObjectId(value);
					} catch (ConversionException ignored) {
					}
				}
				newKey = "_id";
			} else if (key.startsWith("$") && key.endsWith("or")) {
				// $or/$nor
				BasicBSONList conditions = (BasicBSONList) value;
				BasicBSONList newConditions = new BasicBSONList();
				Iterator<Object> iter = conditions.iterator();
				while (iter.hasNext()) {
					newConditions.add(getMappedObject((DBObject) iter.next(), entity));
				}
				value = newConditions;
			} else {
				// TODO: Implement other forms of conversion (like @Alias and whatnot)
			}
			newDbo.put(newKey, value);
		}
		return newDbo;
	}

}
