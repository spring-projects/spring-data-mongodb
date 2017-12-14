/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2017/12
 */
abstract class AbstractJsonSchemaObject implements JsonSchemaObject {

	protected final Set<Type> types;
	protected final @Nullable String description;
	protected final Restrictions restrictions;

	public AbstractJsonSchemaObject(@Nullable Type type, @Nullable String description,
			@Nullable Restrictions restrictions) {
		this(type != null ? Collections.singleton(type) : Collections.emptySet(), description, restrictions);
	}

	public AbstractJsonSchemaObject(Set<Type> types, @Nullable String description, @Nullable Restrictions restrictions) {

		Assert.notNull(types, "Types must not be null! Please consider using 'Collections.emptySet()'.");

		this.types = types;
		this.description = description;
		this.restrictions = restrictions != null ? restrictions : Restrictions.empty();
	}

	@Override
	public Set<Type> getTypes() {
		return types;
	}

	@Override
	public Document toDocument() {

		Document document = new Document();

		if (!CollectionUtils.isEmpty(types)) {

			Type theType = types.iterator().next();
			if (types.size() == 1) {
				document.append(theType.representation(), theType.value());
			} else {
				document.append(theType.representation(), types.stream().map(Type::value).collect(Collectors.toList()));
			}
		}

		getOrCreateDescription().ifPresent(val -> document.append("description", val));

		if (restrictions != null) {
			document.putAll(restrictions.toDocument());
		}

		return document;
	}

	private Optional<String> getOrCreateDescription() {

		if (StringUtils.hasText(description)) {
			return Optional.of(description);
		}

		return Optional.ofNullable(generateDescription());
	}

	@Nullable
	protected String generateDescription() {
		return null;
	}

	public abstract JsonSchemaObject possibleValues(Collection<Object> possibleValues);

	public abstract JsonSchemaObject allOf(Collection<JsonSchemaObject> allOf);

	public abstract JsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf);

	public abstract JsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf);

	public abstract JsonSchemaObject notMatch(JsonSchemaObject notMatch);

	public abstract JsonSchemaObject description(String description);

	static class Restrictions {

		private final Collection<Object> possibleValues;
		private final Collection<JsonSchemaObject> allOf;
		private final Collection<JsonSchemaObject> anyOf;
		private final Collection<JsonSchemaObject> oneOf;
		private final @Nullable JsonSchemaObject notMatch;

		protected Restrictions(Collection<Object> possibleValues, Collection<JsonSchemaObject> allOf,
				Collection<JsonSchemaObject> anyOf, Collection<JsonSchemaObject> oneOf, JsonSchemaObject notMatch) {

			this.possibleValues = possibleValues;
			this.allOf = allOf;
			this.anyOf = anyOf;
			this.oneOf = oneOf;
			this.notMatch = notMatch;
		}

		static Restrictions empty() {

			return new Restrictions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
					Collections.emptySet(), null);
		}

		Restrictions possibleValues(Collection<Object> possibleValues) {
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		Restrictions allOf(Collection<JsonSchemaObject> allOf) {
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		Restrictions anyOf(Collection<JsonSchemaObject> anyOf) {
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		Restrictions oneOf(Collection<JsonSchemaObject> oneOf) {
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		Restrictions notMatch(JsonSchemaObject notMatch) {
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		public Document toDocument() {

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
				document.append("notMatch", notMatch.toDocument());
			}

			return document;
		}
	}

}
