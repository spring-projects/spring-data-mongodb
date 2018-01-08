/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Common base for {@link JsonSchemaObject} with shared types and {@link JsonSchemaObject#toDocument()} implementation.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class UntypedJsonSchemaObject implements JsonSchemaObject {

	protected final @Nullable String description;
	protected final Restrictions restrictions;
	protected final boolean generateDescription;

	protected UntypedJsonSchemaObject(Restrictions restrictions, @Nullable String description, boolean generateDescription) {

		this.description = description;
		this.restrictions = restrictions != null ? restrictions : Restrictions.empty();
		this.generateDescription = generateDescription;
	}

	public static UntypedJsonSchemaObject newInstance() {
		return new UntypedJsonSchemaObject(null, null, false);
	}

	@Override
	public Set<Type> getTypes() {
		return Collections.emptySet();
	}

	/**
	 * Set the {@literal description}.
	 *
	 * @param description must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject description(String description) {
		return new UntypedJsonSchemaObject(restrictions, description, generateDescription);
	}

	/**
	 * Auto generate the {@literal description} if not explicitly set.
	 *
	 * @param description must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject generatedDescription() {
		return new UntypedJsonSchemaObject(restrictions, description, true);
	}

	/**
	 * {@literal enum}erates all possible values of the field.
	 *
	 * @param possibleValues must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
		return new UntypedJsonSchemaObject(restrictions.possibleValues(possibleValues), description, generateDescription);
	}

	/**
	 * The field value must match all specified schemas.
	 *
	 * @param allOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
		return new UntypedJsonSchemaObject(restrictions.allOf(allOf), description, generateDescription);
	}

	/**
	 * The field value must match at least one of the specified schemas.
	 *
	 * @param anyOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
		return new UntypedJsonSchemaObject(restrictions.anyOf(anyOf), description, generateDescription);
	}

	/**
	 * The field value must match exactly one of the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
		return new UntypedJsonSchemaObject(restrictions.oneOf(oneOf), description, generateDescription);
	}

	/**
	 * The field value must not match the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public UntypedJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
		return new UntypedJsonSchemaObject(restrictions.notMatch(notMatch), description, generateDescription);
	}

	/**
	 * Create the json schema complying {@link Document} representation. This includes {@literal type},
	 * {@literal description} and the fields of {@link Restrictions#toDocument()} if set.
	 */
	@Override
	public Document toDocument() {

		Document document = new Document();

		getOrCreateDescription().ifPresent(val -> document.append("description", val));

		if (restrictions != null) {
			document.putAll(restrictions.toDocument());
		}

		return document;
	}

	private Optional<String> getOrCreateDescription() {

		if (description != null) {
			return description.isEmpty() ? Optional.empty() : Optional.of(description);
		}

		return generateDescription ? Optional.ofNullable(generateDescription()) : Optional.empty();
	}

	/**
	 * Customization hook for creating description out of defined values.<br />
	 * Called by {@link #toDocument()} when no explicit {@link #description} is set.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	protected String generateDescription() {
		return null;
	}

	/**
	 * {@link Restrictions} encapsulate common json schema restrictions like {@literal enum}, {@literal allOf}, ...
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class Restrictions {

		private final Collection<Object> possibleValues;
		private final Collection<JsonSchemaObject> allOf;
		private final Collection<JsonSchemaObject> anyOf;
		private final Collection<JsonSchemaObject> oneOf;
		private final @Nullable JsonSchemaObject notMatch;

		Restrictions(Collection<Object> possibleValues, Collection<JsonSchemaObject> allOf,
				Collection<JsonSchemaObject> anyOf, Collection<JsonSchemaObject> oneOf, @Nullable JsonSchemaObject notMatch) {

			this.possibleValues = possibleValues;
			this.allOf = allOf;
			this.anyOf = anyOf;
			this.oneOf = oneOf;
			this.notMatch = notMatch;
		}

		/**
		 * @return new empty {@link Restrictions}.
		 */
		static Restrictions empty() {

			return new Restrictions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
					Collections.emptySet(), null);
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return
		 */
		Restrictions possibleValues(Collection<Object> possibleValues) {

			Assert.notNull(possibleValues, "PossibleValues must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return
		 */
		Restrictions allOf(Collection<JsonSchemaObject> allOf) {

			Assert.notNull(allOf, "AllOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return
		 */
		Restrictions anyOf(Collection<JsonSchemaObject> anyOf) {

			Assert.notNull(anyOf, "AnyOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return
		 */
		Restrictions oneOf(Collection<JsonSchemaObject> oneOf) {

			Assert.notNull(oneOf, "OneOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return
		 */
		Restrictions notMatch(JsonSchemaObject notMatch) {

			Assert.notNull(notMatch, "NotMatch must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * Create the json schema complying {@link Document} representation. This includes {@literal enum},
		 * {@literal allOf}, {@literal anyOf}, {@literal oneOf}, {@literal notMatch} if set.
		 *
		 * @return never {@literal null}
		 */
		Document toDocument() {

			Document document = new Document();

			if (!CollectionUtils.isEmpty(possibleValues)) {
				document.append("enum", possibleValues);
			}
			if (!CollectionUtils.isEmpty(allOf)) {
				document.append("allOf", allOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (!CollectionUtils.isEmpty(anyOf)) {
				document.append("anyOf", anyOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (!CollectionUtils.isEmpty(oneOf)) {
				document.append("oneOf", oneOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (notMatch != null) {
				document.append("not", notMatch.toDocument());
			}

			return document;
		}
	}
}
