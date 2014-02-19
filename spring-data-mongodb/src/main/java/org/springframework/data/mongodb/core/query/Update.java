/*
 * Copyright 2010-2014 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Class to easily construct MongoDB update clauses.
 * 
 * @author Thomas Risberg
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Becca Gaspard
 * @author Christoph Strobl
 */
public class Update {

	public enum Position {
		LAST, FIRST
	}

	private Set<String> keysToUpdate = new HashSet<String>();
	private Map<String, Object> modifierOps = new LinkedHashMap<String, Object>();
	private Map<String, PushOperatorBuilder> pushCommandBuilders = new LinkedHashMap<String, PushOperatorBuilder>(1);

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
	 * Creates an {@link Update} instance from the given {@link DBObject}. Allows to explicitly exlude fields from making
	 * it into the created {@link Update} object. Note, that this will set attributes directly and <em>not</em> use
	 * {@literal $set}. This means fields not given in the {@link DBObject} will be nulled when executing the update. To
	 * create an only-updating {@link Update} instance of a {@link DBObject}, call {@link #set(String, Object)} for each
	 * value in it.
	 * 
	 * @param object the source {@link DBObject} to create the update from.
	 * @param exclude the fields to exclude.
	 * @return
	 */
	public static Update fromDBObject(DBObject object, String... exclude) {

		Update update = new Update();
		List<String> excludeList = Arrays.asList(exclude);

		for (String key : object.keySet()) {

			if (excludeList.contains(key)) {
				continue;
			}

			Object value = object.get(key);
			update.modifierOps.put(key, value);
			if (isKeyword(key) && value instanceof DBObject) {
				update.keysToUpdate.addAll(((DBObject) value).keySet());
			} else {
				update.keysToUpdate.add(key);
			}
		}

		return update;
	}

	/**
	 * Update using the $set update modifier
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Update set(String key, Object value) {
		addMultiFieldOperation("$set", key, value);
		return this;
	}

	/**
	 * Update using the $setOnInsert update modifier
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Update setOnInsert(String key, Object value) {
		addMultiFieldOperation("$setOnInsert", key, value);
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
		addMultiFieldOperation("$push", key, value);
		return this;
	}

	/**
	 * Update using {@code $push} modifier. <br/>
	 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values.
	 * 
	 * @param key
	 * @return {@link PushOperatorBuilder} for given key
	 */
	public PushOperatorBuilder push(String key) {

		if (!pushCommandBuilders.containsKey(key)) {
			pushCommandBuilders.put(key, new PushOperatorBuilder(key));
		}
		return pushCommandBuilders.get(key);
	}

	/**
	 * Update using the {@code $pushAll} update modifier. <br>
	 * <b>Note</b>: In mongodb 2.4 the usage of {@code $pushAll} has been deprecated in favor of {@code $push $each}.
	 * {@link #push(String)}) returns a builder that can be used to populate the {@code $each} object.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/update/pushAll/
	 * @param key
	 * @param values
	 * @return
	 */
	public Update pushAll(String key, Object[] values) {

		Object[] convertedValues = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			convertedValues[i] = values[i];
		}
		addMultiFieldOperation("$pushAll", key, convertedValues);
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
		addMultiFieldOperation("$addToSet", key, value);
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
		addMultiFieldOperation("$pop", key, pos == Position.FIRST ? -1 : 1);
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
		addMultiFieldOperation("$pull", key, value);
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
			convertedValues[i] = values[i];
		}
		addFieldOperation("$pullAll", key, convertedValues);
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

	public DBObject getUpdateObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : modifierOps.keySet()) {
			dbo.put(k, modifierOps.get(k));
		}
		return dbo;
	}

	protected void addFieldOperation(String operator, String key, Object value) {

		Assert.hasText(key, "Key/Path for update must not be null or blank.");
		modifierOps.put(operator, new BasicDBObject(key, value));
		this.keysToUpdate.add(key);
	}

	protected void addMultiFieldOperation(String operator, String key, Object value) {

		Assert.hasText(key, "Key/Path for update must not be null or blank.");
		Object existingValue = this.modifierOps.get(operator);
		DBObject keyValueMap;

		if (existingValue == null) {
			keyValueMap = new BasicDBObject();
			this.modifierOps.put(operator, keyValueMap);
		} else {
			if (existingValue instanceof BasicDBObject) {
				keyValueMap = (BasicDBObject) existingValue;
			} else {
				throw new InvalidDataAccessApiUsageException("Modifier Operations should be a LinkedHashMap but was "
						+ existingValue.getClass());
			}
		}

		keyValueMap.put(key, value);
		this.keysToUpdate.add(key);
	}

	/**
	 * Determine if a given {@code key} will be touched on execution.
	 * 
	 * @param key
	 * @return
	 */
	public boolean modifies(String key) {
		return this.keysToUpdate.contains(key);
	}

	/**
	 * Inspects given {@code key} for '$'.
	 * 
	 * @param key
	 * @return
	 */
	private static boolean isKeyword(String key) {
		return StringUtils.startsWithIgnoreCase(key, "$");
	}

	/**
	 * Modifiers holds a distinct collection of {@link Modifier}
	 * 
	 * @author Christoph Strobl
	 */
	public static class Modifiers {

		private HashMap<String, Modifier> modifiers;

		public Modifiers() {
			this.modifiers = new LinkedHashMap<String, Modifier>(1);
		}

		public Collection<Modifier> getModifiers() {
			return Collections.unmodifiableCollection(this.modifiers.values());
		}

		public void addModifier(Modifier modifier) {
			this.modifiers.put(modifier.getKey(), modifier);
		}
	}

	/**
	 * Marker interface of nested commands.
	 * 
	 * @author Christoph Strobl
	 */
	public static interface Modifier {

		/**
		 * @return the command to send eg. {@code $push}
		 */
		String getKey();

		/**
		 * @return value to be sent with command
		 */
		Object getValue();
	}

	/**
	 * Implementation of {@link Modifier} representing {@code $each}.
	 * 
	 * @author Christoph Strobl
	 */
	private static class Each implements Modifier {

		private Object[] values;

		public Each(Object... values) {
			this.values = extractValues(values);
		}

		private Object[] extractValues(Object[] values) {

			if (values == null || values.length == 0) {
				return values;
			}

			if (values.length == 1 && values[0] instanceof Collection) {
				return ((Collection<?>) values[0]).toArray();
			}

			Object[] convertedValues = new Object[values.length];
			for (int i = 0; i < values.length; i++) {
				convertedValues[i] = values[i];
			}

			return convertedValues;
		}

		@Override
		public String getKey() {
			return "$each";
		}

		@Override
		public Object getValue() {
			return this.values;
		}
	}

	/**
	 * Builder for creating {@code $push} modifiers
	 * 
	 * @author Christop Strobl
	 */
	public class PushOperatorBuilder {

		private final String key;
		private final Modifiers modifiers;

		PushOperatorBuilder(String key) {
			this.key = key;
			this.modifiers = new Modifiers();
		}

		/**
		 * Propagates {@code $each} to {@code $push}
		 * 
		 * @param values
		 * @return
		 */
		public Update each(Object... values) {

			this.modifiers.addModifier(new Each(values));
			return Update.this.push(key, this.modifiers);
		}

		/**
		 * Propagates {@link #value(Object)} to {@code $push}
		 * 
		 * @param values
		 * @return
		 */
		public Update value(Object value) {
			return Update.this.push(key, value);
		}
	}
}
