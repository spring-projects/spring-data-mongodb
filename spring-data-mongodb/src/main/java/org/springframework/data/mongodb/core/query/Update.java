/*
 * Copyright 2010-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bson.Document;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Class to easily construct MongoDB update clauses.
 *
 * @author Thomas Risberg
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Becca Gaspard
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Alexey Plotnik
 * @author Mark Paluch
 * @author Pavel Vodrazka
 */
public class Update implements UpdateDefinition {

	public enum Position {
		LAST, FIRST
	}

	private boolean isolated = false;
	private final Set<String> keysToUpdate = new HashSet<>();
	private final Map<String, Object> modifierOps = new LinkedHashMap<>();
	private Map<String, PushOperatorBuilder> pushCommandBuilders = Collections.emptyMap();
	private List<ArrayFilter> arrayFilters = Collections.emptyList();

	/**
	 * Static factory method to create an Update using the provided key
	 *
	 * @param key the field to update.
	 * @return new instance of {@link Update}.
	 */
	public static Update update(String key, @Nullable Object value) {
		return new Update().set(key, value);
	}

	/**
	 * Creates an {@link Update} instance from the given {@link Document}. Allows to explicitly exclude fields from making
	 * it into the created {@link Update} object. Note, that this will set attributes directly and <em>not</em> use
	 * {@literal $set}. This means fields not given in the {@link Document} will be nulled when executing the update. To
	 * create an only-updating {@link Update} instance of a {@link Document}, call {@link #set(String, Object)} for each
	 * value in it.
	 *
	 * @param object the source {@link Document} to create the update from.
	 * @param exclude the fields to exclude.
	 * @return new instance of {@link Update}.
	 */
	public static Update fromDocument(Document object, String... exclude) {

		Update update = new Update();
		List<String> excludeList = Arrays.asList(exclude);

		for (String key : object.keySet()) {

			if (excludeList.contains(key)) {
				continue;
			}

			Object value = object.get(key);
			update.modifierOps.put(key, value);
			if (isKeyword(key) && value instanceof Document document) {
				update.keysToUpdate.addAll(document.keySet());
			} else {
				update.keysToUpdate.add(key);
			}
		}

		return update;
	}

	/**
	 * Update using the {@literal $set} update modifier
	 *
	 * @param key the field name.
	 * @param value can be {@literal null}. In this case the property remains in the db with a {@literal null} value. To
	 *          remove it use {@link #unset(String)}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/update/set/">MongoDB Update operator: $set</a>
	 */
	public Update set(String key, @Nullable Object value) {
		addMultiFieldOperation("$set", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $setOnInsert} update modifier
	 *
	 * @param key the field name.
	 * @param value can be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/setOnInsert/">MongoDB Update operator:
	 *      $setOnInsert</a>
	 */
	public Update setOnInsert(String key, @Nullable Object value) {
		addMultiFieldOperation("$setOnInsert", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $unset} update modifier
	 *
	 * @param key the field name.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/unset/">MongoDB Update operator: $unset</a>
	 */
	public Update unset(String key) {
		addMultiFieldOperation("$unset", key, 1);
		return this;
	}

	/**
	 * Update using the {@literal $inc} update modifier
	 *
	 * @param key the field name.
	 * @param inc must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/inc/">MongoDB Update operator: $inc</a>
	 */
	public Update inc(String key, Number inc) {
		addMultiFieldOperation("$inc", key, inc);
		return this;
	}

	@Override
	public void inc(String key) {
		inc(key, 1L);
	}

	/**
	 * Update using the {@literal $push} update modifier
	 *
	 * @param key the field name.
	 * @param value can be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/push/">MongoDB Update operator: $push</a>
	 */
	public Update push(String key, @Nullable Object value) {
		addMultiFieldOperation("$push", key, value);
		return this;
	}

	/**
	 * Update using {@code $push} modifier. <br/>
	 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values as well as using
	 * {@code $position}.
	 *
	 * @param key the field name.
	 * @return {@link PushOperatorBuilder} for given key
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/push/">MongoDB Update operator: $push</a>
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/each/">MongoDB Update operator: $each</a>
	 */
	public PushOperatorBuilder push(String key) {

		if (!pushCommandBuilders.containsKey(key)) {

			if (pushCommandBuilders == Collections.EMPTY_MAP) {
				pushCommandBuilders = new LinkedHashMap<>(1);
			}

			pushCommandBuilders.put(key, new PushOperatorBuilder(key));
		}
		return pushCommandBuilders.get(key);
	}

	/**
	 * Update using {@code $addToSet} modifier. <br/>
	 * Allows creation of {@code $push} command for single or multiple (using {@code $each}) values
	 *
	 * @param key the field name.
	 * @return new instance of {@link AddToSetBuilder}.
	 * @since 1.5
	 */
	public AddToSetBuilder addToSet(String key) {
		return new AddToSetBuilder(key);
	}

	/**
	 * Update using the {@literal $addToSet} update modifier
	 *
	 * @param key the field name.
	 * @param value can be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/addToSet/">MongoDB Update operator:
	 *      $addToSet</a>
	 */
	public Update addToSet(String key, @Nullable Object value) {
		addMultiFieldOperation("$addToSet", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $pop} update modifier
	 *
	 * @param key the field name.
	 * @param pos must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/pop/">MongoDB Update operator: $pop</a>
	 */
	public Update pop(String key, Position pos) {
		addMultiFieldOperation("$pop", key, pos == Position.FIRST ? -1 : 1);
		return this;
	}

	/**
	 * Update using the {@literal $pull} update modifier
	 *
	 * @param key the field name.
	 * @param value can be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/pull/">MongoDB Update operator: $pull</a>
	 */
	public Update pull(String key, @Nullable Object value) {
		addMultiFieldOperation("$pull", key, value);
		return this;
	}

	/**
	 * Update using the {@literal $pullAll} update modifier
	 *
	 * @param key the field name.
	 * @param values must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/pullAll/">MongoDB Update operator:
	 *      $pullAll</a>
	 */
	public Update pullAll(String key, Object[] values) {
		addMultiFieldOperation("$pullAll", key, Arrays.asList(values));
		return this;
	}

	/**
	 * Update using the {@literal $rename} update modifier
	 *
	 * @param oldName must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/rename/">MongoDB Update operator:
	 *      $rename</a>
	 */
	public Update rename(String oldName, String newName) {
		addMultiFieldOperation("$rename", oldName, newName);
		return this;
	}

	/**
	 * Update given key to current date using {@literal $currentDate} modifier.
	 *
	 * @param key the field name.
	 * @return this.
	 * @since 1.6
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/currentDate/">MongoDB Update operator:
	 *      $currentDate</a>
	 */
	public Update currentDate(String key) {

		addMultiFieldOperation("$currentDate", key, true);
		return this;
	}

	/**
	 * Update given key to current date using {@literal $currentDate : &#123; $type : "timestamp" &#125;} modifier.
	 *
	 * @param key the field name.
	 * @return this.
	 * @since 1.6
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/currentDate/">MongoDB Update operator:
	 *      $currentDate</a>
	 */
	public Update currentTimestamp(String key) {

		addMultiFieldOperation("$currentDate", key, new Document("$type", "timestamp"));
		return this;
	}

	/**
	 * Multiply the value of given key by the given number.
	 *
	 * @param key must not be {@literal null}.
	 * @param multiplier must not be {@literal null}.
	 * @return this.
	 * @since 1.7
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/mul/">MongoDB Update operator: $mul</a>
	 */
	public Update multiply(String key, Number multiplier) {

		Assert.notNull(multiplier, "Multiplier must not be null");
		addMultiFieldOperation("$mul", key, multiplier.doubleValue());
		return this;
	}

	/**
	 * Update given key to the {@code value} if the {@code value} is greater than the current value of the field.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return this.
	 * @since 1.10
	 * @see <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/">Comparison/Sort Order</a>
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/max/">MongoDB Update operator: $max</a>
	 */
	public Update max(String key, Object value) {

		Assert.notNull(value, "Value for max operation must not be null");
		addMultiFieldOperation("$max", key, value);
		return this;
	}

	/**
	 * Update given key to the {@code value} if the {@code value} is less than the current value of the field.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return this.
	 * @since 1.10
	 * @see <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/">Comparison/Sort Order</a>
	 * @see <a href="https://docs.mongodb.org/manual/reference/operator/update/min/">MongoDB Update operator: $min</a>
	 */
	public Update min(String key, Object value) {

		Assert.notNull(value, "Value for min operation must not be null");
		addMultiFieldOperation("$min", key, value);
		return this;
	}

	/**
	 * The operator supports bitwise {@code and}, bitwise {@code or}, and bitwise {@code xor} operations.
	 *
	 * @param key the field name.
	 * @return this.
	 * @since 1.7
	 */
	public BitwiseOperatorBuilder bitwise(String key) {
		return new BitwiseOperatorBuilder(this, key);
	}

	/**
	 * Prevents a write operation that affects <strong>multiple</strong> documents from yielding to other reads or writes
	 * once the first document is written. <br />
	 * Use with {@link org.springframework.data.mongodb.core.MongoOperations#updateMulti(Query, UpdateDefinition, Class)}.
	 *
	 * @return this.
	 * @since 2.0
	 */
	public Update isolated() {

		isolated = true;
		return this;
	}

	/**
	 * Filter elements in an array that match the given criteria for update. {@link CriteriaDefinition} is passed directly
	 * to the driver without further type or field mapping.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return this.
	 * @since 2.2
	 */
	public Update filterArray(CriteriaDefinition criteria) {

		if (arrayFilters == Collections.EMPTY_LIST) {
			this.arrayFilters = new ArrayList<>();
		}

		this.arrayFilters.add(criteria::getCriteriaObject);
		return this;
	}

	/**
	 * Filter elements in an array that match the given criteria for update. {@code expression} is used directly with the
	 * driver without further further type or field mapping.
	 *
	 * @param identifier the positional operator identifier filter criteria name.
	 * @param expression the positional operator filter expression.
	 * @return this.
	 * @since 2.2
	 */
	public Update filterArray(String identifier, Object expression) {

		if (arrayFilters == Collections.EMPTY_LIST) {
			this.arrayFilters = new ArrayList<>();
		}

		this.arrayFilters.add(() -> new Document(identifier, expression));
		return this;
	}

	public Boolean isIsolated() {
		return isolated;
	}

	public Document getUpdateObject() {
		return new Document(modifierOps);
	}

	public List<ArrayFilter> getArrayFilters() {
		return Collections.unmodifiableList(this.arrayFilters);
	}

	@Override
	public boolean hasArrayFilters() {
		return !this.arrayFilters.isEmpty();
	}

	protected void addMultiFieldOperation(String operator, String key, @Nullable Object value) {

		Assert.hasText(key, "Key/Path for update must not be null or blank");
		Object existingValue = this.modifierOps.get(operator);
		Document keyValueMap;

		if (existingValue == null) {
			keyValueMap = new Document();
			this.modifierOps.put(operator, keyValueMap);
		} else {
			if (existingValue instanceof Document document) {
				keyValueMap = document;
			} else {
				throw new InvalidDataAccessApiUsageException(
						"Modifier Operations should be a LinkedHashMap but was " + existingValue.getClass());
			}
		}

		keyValueMap.put(key, value);
		this.keysToUpdate.add(key);
	}

	/**
	 * Determine if a given {@code key} will be touched on execution.
	 *
	 * @param key the field name.
	 * @return {@literal true} if given field is updated.
	 */
	public boolean modifies(String key) {
		return this.keysToUpdate.contains(key);
	}

	/**
	 * Inspects given {@code key} for '$'.
	 *
	 * @param key the field name.
	 * @return {@literal true} if given key is prefixed.
	 */
	private static boolean isKeyword(String key) {
		return StringUtils.startsWithIgnoreCase(key, "$");
	}

	@Override
	public int hashCode() {
		return Objects.hash(getUpdateObject(), isolated);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		Update that = (Update) obj;
		if (this.isolated != that.isolated) {
			return false;
		}

		return Objects.equals(this.getUpdateObject(), that.getUpdateObject());
	}

	@Override
	public String toString() {

		Document doc = getUpdateObject();

		if (isIsolated()) {
			doc.append("$isolated", 1);
		}

		return SerializationUtils.serializeToJsonSafely(doc);
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
			this.modifiers = new LinkedHashMap<>(1);
		}

		public Collection<Modifier> getModifiers() {
			return Collections.unmodifiableCollection(this.modifiers.values());
		}

		public void addModifier(Modifier modifier) {
			this.modifiers.put(modifier.getKey(), modifier);
		}

		/**
		 * @return true if no modifiers present.
		 * @since 2.0
		 */
		public boolean isEmpty() {
			return modifiers.isEmpty();
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(modifiers);
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			Modifiers that = (Modifiers) obj;
			return Objects.equals(this.modifiers, that.modifiers);
		}

		@Override
		public String toString() {
			return SerializationUtils.serializeToJsonSafely(this.modifiers);
		}
	}

	/**
	 * Marker interface of nested commands.
	 *
	 * @author Christoph Strobl
	 */
	public interface Modifier {

		/**
		 * @return the command to send eg. {@code $push}
		 */
		String getKey();

		/**
		 * @return value to be sent with command
		 */
		Object getValue();

		/**
		 * @return a safely serialized JSON representation.
		 * @since 2.0
		 */
		default String toJsonString() {
			return SerializationUtils.serializeToJsonSafely(Collections.singletonMap(getKey(), getValue()));
		}
	}

	/**
	 * Abstract {@link Modifier} implementation with defaults for {@link Object#equals(Object)}, {@link Object#hashCode()}
	 * and {@link Object#toString()}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	private static abstract class AbstractModifier implements Modifier {

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(getKey()) + ObjectUtils.nullSafeHashCode(getValue());
		}

		@Override
		public boolean equals(@Nullable Object that) {

			if (this == that) {
				return true;
			}

			if (that == null || getClass() != that.getClass()) {
				return false;
			}

			if (!Objects.equals(getKey(), ((Modifier) that).getKey())) {
				return false;
			}

			return Objects.deepEquals(getValue(), ((Modifier) that).getValue());
		}

		@Override
		public String toString() {
			return toJsonString();
		}
	}

	/**
	 * Implementation of {@link Modifier} representing {@code $each}.
	 *
	 * @author Christoph Strobl
	 * @author Thomas Darimont
	 */
	private static class Each extends AbstractModifier {

		private Object[] values;

		Each(Object... values) {
			this.values = extractValues(values);
		}

		private Object[] extractValues(Object[] values) {

			if (values == null || values.length == 0) {
				return values;
			}

			if (values.length == 1 && values[0] instanceof Collection<?> collection) {
				return collection.toArray();
			}

			return Arrays.copyOf(values, values.length);
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
	 * {@link Modifier} implementation used to propagate {@code $position}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class PositionModifier extends AbstractModifier {

		private final int position;

		PositionModifier(int position) {
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
	 * Implementation of {@link Modifier} representing {@code $slice}.
	 *
	 * @author Mark Paluch
	 * @since 1.10
	 */
	private static class Slice extends AbstractModifier {

		private int count;

		Slice(int count) {
			this.count = count;
		}

		@Override
		public String getKey() {
			return "$slice";
		}

		@Override
		public Object getValue() {
			return this.count;
		}
	}

	/**
	 * Implementation of {@link Modifier} representing {@code $sort}.
	 *
	 * @author Pavel Vodrazka
	 * @author Mark Paluch
	 * @since 1.10
	 */
	private static class SortModifier extends AbstractModifier {

		private final Object sort;

		/**
		 * Creates a new {@link SortModifier} instance given {@link Direction}.
		 *
		 * @param direction must not be {@literal null}.
		 */
		SortModifier(Direction direction) {

			Assert.notNull(direction, "Direction must not be null");
			this.sort = direction.isAscending() ? 1 : -1;
		}

		/**
		 * Creates a new {@link SortModifier} instance given {@link Sort}.
		 *
		 * @param sort must not be {@literal null}.
		 */
		SortModifier(Sort sort) {

			Assert.notNull(sort, "Sort must not be null");

			for (Order order : sort) {

				if (order.isIgnoreCase()) {
					throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case;"
							+ " MongoDB does not support sorting ignoring case currently", order.getProperty()));
				}
			}

			this.sort = sort;
		}

		@Override
		public String getKey() {
			return "$sort";
		}

		@Override
		public Object getValue() {
			return this.sort;
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
		 * @return never {@literal null}.
		 */
		public Update each(Object... values) {

			this.modifiers.addModifier(new Each(values));
			return Update.this.push(key, this.modifiers);
		}

		/**
		 * Propagates {@code $slice} to {@code $push}. {@code $slice} requires the {@code $each operator}. <br />
		 * If {@literal count} is zero, {@code $slice} updates the array to an empty array. <br />
		 * If {@literal count} is negative, {@code $slice} updates the array to contain only the last {@code count}
		 * elements. <br />
		 * If {@literal count} is positive, {@code $slice} updates the array to contain only the first {@code count}
		 * elements. <br />
		 *
		 * @param count
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public PushOperatorBuilder slice(int count) {

			this.modifiers.addModifier(new Slice(count));
			return this;
		}

		/**
		 * Propagates {@code $sort} to {@code $push}. {@code $sort} requires the {@code $each} operator. Forces elements to
		 * be sorted by values in given {@literal direction}.
		 *
		 * @param direction must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public PushOperatorBuilder sort(Direction direction) {

			Assert.notNull(direction, "Direction must not be null");
			this.modifiers.addModifier(new SortModifier(direction));
			return this;
		}

		/**
		 * Propagates {@code $sort} to {@code $push}. {@code $sort} requires the {@code $each} operator. Forces document
		 * elements to be sorted in given {@literal order}.
		 *
		 * @param sort must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 1.10
		 */
		public PushOperatorBuilder sort(Sort sort) {

			Assert.notNull(sort, "Sort must not be null");
			this.modifiers.addModifier(new SortModifier(sort));
			return this;
		}

		/**
		 * Forces values to be added at the given {@literal position}.
		 *
		 * @param position the position offset. As of MongoDB 3.6 use a negative value to indicate starting from the end,
		 *          counting (but not including) the last element of the array.
		 * @return never {@literal null}.
		 * @since 1.7
		 */
		public PushOperatorBuilder atPosition(int position) {

			this.modifiers.addModifier(new PositionModifier(position));
			return this;
		}

		/**
		 * Forces values to be added at given {@literal position}.
		 *
		 * @param position can be {@literal null} which will be appended at the last position.
		 * @return never {@literal null}.
		 * @since 1.7
		 */
		public PushOperatorBuilder atPosition(@Nullable Position position) {

			if (position == null || Position.LAST.equals(position)) {
				return this;
			}

			this.modifiers.addModifier(new PositionModifier(0));

			return this;
		}

		/**
		 * Propagates {@link #value(Object)} to {@code $push}
		 *
		 * @param value
		 * @return never {@literal null}.
		 */
		public Update value(Object value) {

			if (this.modifiers.isEmpty()) {
				return Update.this.push(key, value);
			}

			this.modifiers.addModifier(new Each(Collections.singletonList(value)));
			return Update.this.push(key, this.modifiers);
		}

		@Override
		public int hashCode() {
			return Objects.hash(getOuterType(), key, modifiers);
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			PushOperatorBuilder that = (PushOperatorBuilder) obj;

			if (!Objects.equals(getOuterType(), that.getOuterType())) {
				return false;
			}

			return Objects.equals(this.key, that.key) && Objects.equals(this.modifiers, that.modifiers);
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
		 * @param values must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public Update each(Object... values) {
			return Update.this.addToSet(this.key, new Each(values));
		}

		/**
		 * Propagates {@link #value(Object)} to {@code $addToSet}
		 *
		 * @param value
		 * @return never {@literal null}.
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
			}
		}

		/**
		 * Creates a new {@link BitwiseOperatorBuilder}.
		 *
		 * @param reference must not be {@literal null}
		 * @param key must not be {@literal null}
		 */
		protected BitwiseOperatorBuilder(Update reference, String key) {

			Assert.notNull(reference, "Reference must not be null");
			Assert.notNull(key, "Key must not be null");

			this.reference = reference;
			this.key = key;
		}

		/**
		 * Updates to the result of a bitwise and operation between the current value and the given one.
		 *
		 * @param value
		 * @return never {@literal null}.
		 */
		public Update and(long value) {

			addFieldOperation(BitwiseOperator.AND, value);
			return reference;
		}

		/**
		 * Updates to the result of a bitwise or operation between the current value and the given one.
		 *
		 * @param value
		 * @return never {@literal null}.
		 */
		public Update or(long value) {

			addFieldOperation(BitwiseOperator.OR, value);
			return reference;
		}

		/**
		 * Updates to the result of a bitwise xor operation between the current value and the given one.
		 *
		 * @param value
		 * @return never {@literal null}.
		 */
		public Update xor(long value) {

			addFieldOperation(BitwiseOperator.XOR, value);
			return reference;
		}

		private void addFieldOperation(BitwiseOperator operator, Number value) {
			reference.addMultiFieldOperation(BIT_OPERATOR, key, new Document(operator.toString(), value));
		}
	}
}
