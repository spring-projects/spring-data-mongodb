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

import static org.springframework.util.ObjectUtils.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
 * @author Thomas Darimont
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
	 * Creates an {@link Update} instance from the given {@link DBObject}. Allows to explicitly exclude fields from making
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
	 * Update using the {@literal $set} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/set/
	 * @param key
	 * @param value
	 * @return
	 */
	public Update set(String key, Object value) {
		addMultiFieldOperation("$set", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $setOnInsert} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/setOnInsert/
	 * @param key
	 * @param value
	 * @return
	 */
	public Update setOnInsert(String key, Object value) {
		addMultiFieldOperation("$setOnInsert", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $unset} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/unset/
	 * @param key
	 * @return
	 */
	public Update unset(String key) {
		addMultiFieldOperation("$unset", key, 1);
		return this;
	}

	/**
	 * Update using the {@literal $inc} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/inc/
	 * @param key
	 * @param inc
	 * @return
	 */
	public Update inc(String key, Number inc) {
		addMultiFieldOperation("$inc", key, inc);
		return this;
	}

	/**
	 * Update using the {@literal $push} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/push/
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
	 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values as well as using
	 * {@code $position}.
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/push/
	 * @see http://docs.mongodb.org/manual/reference/operator/update/each/
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
		addMultiFieldOperation("$pushAll", key, Arrays.copyOf(values, values.length));
		return this;
	}

	/**
	 * Update using {@code $addToSet} modifier. <br/>
	 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values
	 *
	 * @param key
	 * @return
	 * @since 1.5
	 */
	public AddToSetBuilder addToSet(String key) {
		return new AddToSetBuilder(key);
	}

	/**
	 * Update using the {@literal $addToSet} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/addToSet/
	 * @param key
	 * @param value
	 * @return
	 */
	public Update addToSet(String key, Object value) {
		addMultiFieldOperation("$addToSet", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $pop} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/pop/
	 * @param key
	 * @param pos
	 * @return
	 */
	public Update pop(String key, Position pos) {
		addMultiFieldOperation("$pop", key, pos == Position.FIRST ? -1 : 1);
		return this;
	}

	/**
	 * Update using the {@literal $pull} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/pull/
	 * @param key
	 * @param value
	 * @return
	 */
	public Update pull(String key, Object value) {
		addMultiFieldOperation("$pull", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $pullAll} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/pullAll/
	 * @param key
	 * @param values
	 * @return
	 */
	public Update pullAll(String key, Object[] values) {
		addFieldOperation("$pullAll", key, Arrays.copyOf(values, values.length));
		return this;
	}

	/**
	 * Update using the {@literal $rename} update modifier
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/rename/
	 * @param oldName
	 * @param newName
	 * @return
	 */
	public Update rename(String oldName, String newName) {
		addMultiFieldOperation("$rename", oldName, newName);
		return this;
	}

	/**
	 * Update given key to current date using {@literal $currentDate} modifier.
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/currentDate/
	 * @param key
	 * @return
	 * @since 1.6
	 */
	public Update currentDate(String key) {

		addMultiFieldOperation("$currentDate", key, true);
		return this;
	}

	/**
	 * Update given key to current date using {@literal $currentDate : &#123; $type : "timestamp" &#125;} modifier.
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/currentDate/
	 * @param key
	 * @return
	 * @since 1.6
	 */
	public Update currentTimestamp(String key) {

		addMultiFieldOperation("$currentDate", key, new BasicDBObject("$type", "timestamp"));
		return this;
	}

	/**
	 * Multiply the value of given key by the given number.
	 *
	 * @see http://docs.mongodb.org/manual/reference/operator/update/mul/
	 * @param key must not be {@literal null}.
	 * @param multiplier must not be {@literal null}.
	 * @return
	 * @since 1.7
	 */
	public Update multiply(String key, Number multiplier) {

		Assert.notNull(multiplier, "Multiplier must not be 'null'.");
		addMultiFieldOperation("$mul", key, multiplier.doubleValue());
		return this;
	}

	/**
	 * The operator supports bitwise {@code and}, bitwise {@code or}, and bitwise {@code xor} operations.
	 *
	 * @param key
	 * @return
	 * @since 1.7
	 */
	public BitwiseOperatorBuilder bitwise(String key) {
		return new BitwiseOperatorBuilder(this, key);
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return getUpdateObject().hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		Update that = (Update) obj;
		return this.getUpdateObject().equals(that.getUpdateObject());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(getUpdateObject());
	}

	/**
	 * Modifiers holds a distinct collection of {@link Modifier}
	 *
	 * @author Christoph Strobl
	 * @author Thomas Darimont
	 */
	public static class Modifiers {

		private Map<String, Modifier> modifiers;

		public Modifiers() {
			this.modifiers = new LinkedHashMap<String, Modifier>(1);
		}

		public Collection<Modifier> getModifiers() {
			return Collections.unmodifiableCollection(this.modifiers.values());
		}

		public void addModifier(Modifier modifier) {
			this.modifiers.put(modifier.getKey(), modifier);
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return nullSafeHashCode(modifiers);
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			Modifiers that = (Modifiers) obj;

			return this.modifiers.equals(that.modifiers);
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
	 * @author Thomas Darimont
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

			return Arrays.copyOf(values, values.length);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.query.Update.Modifier#getKey()
		 */
		@Override
		public String getKey() {
			return "$each";
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.query.Update.Modifier#getValue()
		 */
		@Override
		public Object getValue() {
			return this.values;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return nullSafeHashCode(values);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object that) {

			if (this == that) {
				return true;
			}

			if (that == null || getClass() != that.getClass()) {
				return false;
			}

			return nullSafeEquals(values, ((Each) that).values);
		}
	}

	/**
	 * {@link Modifier} implementation used to propagate {@code $position}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class PositionModifier implements Modifier {

		private final int position;

		public PositionModifier(int position) {
			this.position = position;
		}

		@Override
		public String getKey() {
			return "$position";
		}

		@Override
		public Object getValue() {
			return position;
		}
	}

	/**
	 * Builder for creating {@code $push} modifiers
	 *
	 * @author Christoph Strobl
	 * @author Thomas Darimont
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
		 * Forces values to be added at the given {@literal position}.
		 *
		 * @param position needs to be greater than or equal to zero.
		 * @return
		 * @since 1.7
		 */
		public PushOperatorBuilder atPosition(int position) {

			if (position < 0) {
				throw new IllegalArgumentException("Position must be greater than or equal to zero.");
			}

			this.modifiers.addModifier(new PositionModifier(position));

			return this;
		}

		/**
		 * Forces values to be added at given {@literal position}.
		 *
		 * @param position can be {@literal null} which will be appended at the last position.
		 * @return
		 * @since 1.7
		 */
		public PushOperatorBuilder atPosition(Position position) {

			if (position == null || Position.LAST.equals(position)) {
				return this;
			}

			this.modifiers.addModifier(new PositionModifier(0));

			return this;
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

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = 17;

			result += 31 * result + getOuterType().hashCode();
			result += 31 * result + nullSafeHashCode(key);
			result += 31 * result + nullSafeHashCode(modifiers);

			return result;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			PushOperatorBuilder that = (PushOperatorBuilder) obj;

			if (!getOuterType().equals(that.getOuterType())) {
				return false;
			}

			return nullSafeEquals(this.key, that.key) && nullSafeEquals(this.modifiers, that.modifiers);
		}

		private Update getOuterType() {
			return Update.this;
		}
	}

	/**
	 * Builder for creating {@code $addToSet} modifier.
	 *
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	public class AddToSetBuilder {

		private final String key;

		public AddToSetBuilder(String key) {
			this.key = key;
		}

		/**
		 * Propagates {@code $each} to {@code $addToSet}
		 *
		 * @param values
		 * @return
		 */
		public Update each(Object... values) {
			return Update.this.addToSet(this.key, new Each(values));
		}

		/**
		 * Propagates {@link #value(Object)} to {@code $addToSet}
		 *
		 * @param values
		 * @return
		 */
		public Update value(Object value) {
			return Update.this.addToSet(this.key, value);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static class BitwiseOperatorBuilder {

		private final String key;
		private final Update reference;
		private static final String BIT_OPERATOR = "$bit";

		private enum BitwiseOperator {
			AND, OR, XOR;

			@Override
			public String toString() {
				return super.toString().toLowerCase();
			};
		}

		/**
		 * Creates a new {@link BitwiseOperatorBuilder}.
		 *
		 * @param reference must not be {@literal null}
		 * @param key must not be {@literal null}
		 */
		protected BitwiseOperatorBuilder(Update reference, String key) {

			Assert.notNull(reference, "Reference must not be null!");
			Assert.notNull(key, "Key must not be null!");

			this.reference = reference;
			this.key = key;
		}

		/**
		 * Updates to the result of a bitwise and operation between the current value and the given one.
		 *
		 * @param value
		 * @return
		 */
		public Update and(long value) {

			addFieldOperation(BitwiseOperator.AND, value);
			return reference;
		}

		/**
		 * Updates to the result of a bitwise or operation between the current value and the given one.
		 *
		 * @param value
		 * @return
		 */
		public Update or(long value) {

			addFieldOperation(BitwiseOperator.OR, value);
			return reference;
		}

		/**
		 * Updates to the result of a bitwise xor operation between the current value and the given one.
		 *
		 * @param value
		 * @return
		 */
		public Update xor(long value) {

			addFieldOperation(BitwiseOperator.XOR, value);
			return reference;
		}

		private void addFieldOperation(BitwiseOperator operator, Number value) {
			reference.addMultiFieldOperation(BIT_OPERATOR, key, new BasicDBObject(operator.toString(), value));
		}
	}
}
