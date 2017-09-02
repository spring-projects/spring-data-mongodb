/*
 * Copyright 2013-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Value object to capture a list of {@link Field} instances.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @since 1.3
 */
public final class Fields implements Iterable<Field> {

	private static final String AMBIGUOUS_EXCEPTION = "Found two fields both using '%s' as name: %s and %s! Please "
			+ "customize your field definitions to get to unique field names!";

	public static final String UNDERSCORE_ID = "_id";
	public static final String UNDERSCORE_ID_REF = "$_id";

	private final List<Field> fields;

	/**
	 * Creates a new {@link Fields} instance from the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null} or empty.
	 * @return
	 */
	public static Fields from(Field... fields) {

		Assert.notNull(fields, "Fields must not be null!");
		return new Fields(Arrays.asList(fields));
	}

	/**
	 * Creates a new {@link Fields} instance for {@link Field}s with the given names.
	 *
	 * @param names must not be {@literal null}.
	 * @return
	 */
	public static Fields fields(String... names) {

		Assert.notNull(names, "Field names must not be null!");

		List<Field> fields = new ArrayList<Field>();

		for (String name : names) {
			fields.add(field(name));
		}

		return new Fields(fields);
	}

	/**
	 * Creates a {@link Field} with the given name.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	public static Field field(String name) {
		return new AggregationField(name);
	}

	/**
	 * Creates a {@link Field} with the given {@code name} and {@code target}.
	 * <p>
	 * The {@code target} is the name of the backing document field that will be aliased with {@code name}.
	 *
	 * @param name
	 * @param target must not be {@literal null} or empty
	 * @return
	 */
	public static Field field(String name, String target) {
		Assert.hasText(target, "Target must not be null or empty!");
		return new AggregationField(name, target);
	}

	/**
	 * Creates a new {@link Fields} instance using the given {@link Field}s.
	 *
	 * @param fields must not be {@literal null}.
	 */
	private Fields(List<Field> fields) {

		Assert.notNull(fields, "Fields must not be null!");

		this.fields = verify(fields);
	}

	private static final List<Field> verify(List<Field> fields) {

		Map<String, Field> reference = new HashMap<String, Field>();

		for (Field field : fields) {

			String name = field.getName();
			Field found = reference.get(name);

			if (found != null) {
				throw new IllegalArgumentException(String.format(AMBIGUOUS_EXCEPTION, name, found, field));
			}

			reference.put(name, field);
		}

		return fields;
	}

	private Fields(Fields existing, Field tail) {

		this.fields = new ArrayList<Field>(existing.fields.size() + 1);
		this.fields.addAll(existing.fields);
		this.fields.add(tail);
	}

	/**
	 * Creates a new {@link Fields} instance with a new {@link Field} of the given name added.
	 *
	 * @param name must not be {@literal null}.
	 * @return
	 */
	public Fields and(String name) {
		return and(new AggregationField(name));
	}

	public Fields and(String name, String target) {
		return and(new AggregationField(name, target));
	}

	public Fields and(Field field) {
		return new Fields(this, field);
	}

	public Fields and(Fields fields) {

		Fields result = this;

		for (Field field : fields) {
			result = result.and(field);
		}

		return result;
	}

	@Nullable
	public Field getField(String name) {

		for (Field field : fields) {
			if (field.getName().equals(name)) {
				return field;
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Field> iterator() {
		return fields.iterator();
	}

	/**
	 * @return
	 * @since 1.10
	 */
	public List<Field> asList() {
		return Collections.unmodifiableList(fields);
	}

	/**
	 * Value object to encapsulate a field in an aggregation operation.
	 *
	 * @author Oliver Gierke
	 */
	static class AggregationField implements Field {

		private final String raw;
		private final String name;
		private final String target;

		/**
		 * Creates an aggregation field with the given {@code name}.
		 *
		 * @see AggregationField#AggregationField(String, String).
		 * @param name must not be {@literal null} or empty
		 */
		public AggregationField(String name) {
			this(name, null);
		}

		/**
		 * Creates an aggregation field with the given {@code name} and {@code target}.
		 * <p>
		 * The {@code name} serves as an alias for the actual backing document field denoted by {@code target}. If no target
		 * is set explicitly, the name will be used as target.
		 *
		 * @param name must not be {@literal null} or empty
		 * @param target
		 */
		public AggregationField(String name, @Nullable String target) {

			raw = name;
			String nameToSet = name != null ? cleanUp(name) : null;
			String targetToSet = target != null ? cleanUp(target) : null;

			Assert.hasText(nameToSet, "AggregationField name must not be null or empty!");

			if (target == null && name.contains(".")) {
				this.name = nameToSet.substring(nameToSet.indexOf('.') + 1);
				this.target = nameToSet;
			} else {
				this.name = nameToSet;
				this.target = targetToSet;
			}
		}

		private static String cleanUp(String source) {

			if (Aggregation.SystemVariable.isReferingToSystemVariable(source)) {
				return source;
			}

			int dollarIndex = source.lastIndexOf('$');
			return dollarIndex == -1 ? source : source.substring(dollarIndex + 1);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Field#getKey()
		 */
		public String getName() {
			return name;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Field#getAlias()
		 */
		public String getTarget() {

			if (isLocalVar()) {
				return this.getRaw();
			}

			return StringUtils.hasText(this.target) ? this.target : this.name;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Field#isAliased()
		 */
		@Override
		public boolean isAliased() {
			return !getName().equals(getTarget());
		}

		/**
		 * @return {@literal true} in case the field name starts with {@code $$}.
		 * @since 1.10
		 */
		public boolean isLocalVar() {
			return raw.startsWith("$$") && !raw.startsWith("$$$");
		}

		/**
		 * @return
		 * @since 1.10
		 */
		public String getRaw() {
			return raw;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("AggregationField - name: %s, target: %s", name, target);
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

			if (!(obj instanceof AggregationField)) {
				return false;
			}

			AggregationField that = (AggregationField) obj;

			return this.name.equals(that.name) && ObjectUtils.nullSafeEquals(this.target, that.target);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = 17;

			result += 31 * name.hashCode();
			result += 31 * ObjectUtils.nullSafeHashCode(target);

			return result;
		}
	}
}
