/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.document.mongodb.convert.MongoConverter;
import org.springframework.data.mapping.MappingBeanHelper;

public class Update {

	public enum Position {
		LAST, FIRST
	}

	private HashMap<String, Object> modifierOps = new LinkedHashMap<String, Object>();

	/**
	 * Static factory method to create an Update using the provided key
	 *
	 * @param key
	 * @return
	 */
	public static Update update(String key, Object value) {
		return new Update().set(key, value);
	}

	/**
	 * Update using the $set update modifier
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public Update set(String key, Object value) {
		addMultiFieldOperation("$set", key, convertValueIfNecessary(value));
		return this;
	}

	/**
	 * Update using the $unset update modifier
	 *
	 * @param key
	 * @return
	 */
	public Update unset(String key) {
		addMultiFieldOperation("$unset", key, 1);
		return this;
	}

	/**
	 * Update using the $inc update modifier
	 *
	 * @param key
	 * @param inc
	 * @return
	 */
	public Update inc(String key, Number inc) {
		addMultiFieldOperation("$inc", key, inc);
		return this;
	}

	/**
	 * Update using the $push update modifier
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public Update push(String key, Object value) {
		addMultiFieldOperation("$push", key, convertValueIfNecessary(value));
		return this;
	}

	/**
	 * Update using the $pushAll update modifier
	 *
	 * @param key
	 * @param values
	 * @return
	 */
	public Update pushAll(String key, Object[] values) {
		Object[] convertedValues = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			convertedValues[i] = convertValueIfNecessary(values[i]);
		}
		DBObject keyValue = new BasicDBObject();
		keyValue.put(key, convertedValues);
		modifierOps.put("$pushAll", keyValue);
		return this;
	}

	/**
	 * Update using the $addToSet update modifier
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public Update addToSet(String key, Object value) {
		addMultiFieldOperation("$addToSet", key, convertValueIfNecessary(value));
		return this;
	}

	/**
	 * Update using the $pop update modifier
	 *
	 * @param key
	 * @param pos
	 * @return
	 */
	public Update pop(String key, Position pos) {
		addMultiFieldOperation("$pop", key,
				(pos == Position.FIRST ? -1 : 1));
		return this;
	}

	/**
	 * Update using the $pull update modifier
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public Update pull(String key, Object value) {
		addMultiFieldOperation("$pull", key, convertValueIfNecessary(value));
		return this;
	}

	/**
	 * Update using the $pullAll update modifier
	 *
	 * @param key
	 * @param values
	 * @return
	 */
	public Update pullAll(String key, Object[] values) {
		Object[] convertedValues = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			convertedValues[i] = convertValueIfNecessary(values[i]);
		}
		DBObject keyValue = new BasicDBObject();
		keyValue.put(key, convertedValues);
		modifierOps.put("$pullAll", keyValue);
		return this;
	}

	/**
	 * Update using the $rename update modifier
	 *
	 * @param oldName
	 * @param newName
	 * @return
	 */
	public Update rename(String oldName, String newName) {
		addMultiFieldOperation("$rename", oldName, newName);
		return this;
	}

	public DBObject getUpdateObject(MongoConverter converter) {
		DBObject dbo = new BasicDBObject();
		for (String k : modifierOps.keySet()) {
			Object o = modifierOps.get(k);
			if (null != converter) {
				dbo.put(k, maybeConvertObject(o, converter));
			} else {
				dbo.put(k, o);
			}
		}
		return dbo;
	}

	public DBObject getUpdateObject() {
		return getUpdateObject(null);
	}

	@SuppressWarnings("unchecked")
	protected void addMultiFieldOperation(String operator, String key,
																				Object value) {
		Object existingValue = this.modifierOps.get(operator);
		LinkedHashMap<String, Object> keyValueMap;
		if (existingValue == null) {
			keyValueMap = new LinkedHashMap<String, Object>();
			this.modifierOps.put(operator, keyValueMap);
		} else {
			if (existingValue instanceof LinkedHashMap) {
				keyValueMap = (LinkedHashMap<String, Object>) existingValue;
			} else {
				throw new InvalidDataAccessApiUsageException("Modifier Operations should be a LinkedHashMap but was " +
						existingValue.getClass());
			}
		}
		keyValueMap.put(key, value);
	}

	protected Object convertValueIfNecessary(Object value) {
		if (value instanceof Enum) {
			return ((Enum<?>) value).name();
		}
		return value;
	}

	@SuppressWarnings({"unchecked"})
	protected Object maybeConvertObject(Object obj, MongoConverter converter) {
		if (null != obj && MappingBeanHelper.isSimpleType(obj.getClass())) {
			// Doesn't need conversion
			return obj;
		}

		if (obj instanceof Map) {
			Map<Object, Object> m = new HashMap<Object, Object>();
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				m.put(entry.getKey(), maybeConvertObject(entry.getValue(), converter));
			}
			return m;
		}

		if (obj instanceof BasicDBList) {
			return maybeConvertList((BasicDBList) obj, converter);
		}

		if (obj instanceof DBObject) {
			DBObject newValueDbo = new BasicDBObject();
			for (String vk : ((DBObject) obj).keySet()) {
				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, maybeConvertObject(o, converter));
			}
			return newValueDbo;
		}

		DBObject newDbo = new BasicDBObject();
		converter.write(obj, newDbo);
		return newDbo;
	}

	protected BasicDBList maybeConvertList(BasicDBList dbl, MongoConverter converter) {
		BasicDBList newDbl = new BasicDBList();
		Iterator iter = dbl.iterator();
		while (iter.hasNext()) {
			Object o = iter.next();
			newDbl.add(maybeConvertObject(o, converter));
		}
		return newDbl;
	}

}
