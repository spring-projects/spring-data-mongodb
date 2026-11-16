/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.path;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

/**
 * The document shape a {@link MongoPath} is bound against: an entity, the fields an aggregation stage exposes, or
 * nothing known. {@link MongoPathBinder} resolves segments against the provided implementations only, other
 * implementations resolve no segments.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
public interface Scope {

	/**
	 * Create a {@link Scope} for the given entity.
	 *
	 * @param entity must not be {@literal null}.
	 * @return new instance of {@link EntityScope}.
	 */
	static Scope of(MongoPersistentEntity<?> entity) {
		return new EntityScope(entity);
	}

	/**
	 * Create a {@link Scope} for a document shaped by the fields a previous stage exposes.
	 *
	 * @param resolver resolves the first segment of a path, must not be {@literal null}.
	 * @param parent the scope to fall back to for paths not exposed, {@literal null} if the exposed fields replace the
	 *          document.
	 * @return new instance of {@link ExposedFieldsScope}.
	 */
	static Scope exposing(FieldResolver resolver, @Nullable Scope parent) {
		return new ExposedFieldsScope(resolver, parent);
	}

	/**
	 * Create a {@link Scope} exposing a single field, e.g. a variable.
	 *
	 * @param name the field name, must not be {@literal null}.
	 * @param exposure the exposed field, must not be {@literal null}.
	 * @return new instance of {@link ExposedFieldsScope}.
	 */
	static Scope exposing(String name, Exposure exposure) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(exposure, "Exposure must not be null");

		return new ExposedFieldsScope(it -> it.equals(name) ? exposure : null, null);
	}

	/**
	 * @return the untyped {@link Scope}.
	 */
	static Scope none() {
		return NoScope.INSTANCE;
	}

	/**
	 * {@link Scope} backed by a {@link MongoPersistentEntity}.
	 *
	 * @param entity the root entity.
	 */
	record EntityScope(MongoPersistentEntity<?> entity) implements Scope {

		public EntityScope {
			Assert.notNull(entity, "Entity must not be null");
		}

		@Override
		public String toString() {
			return "EntityScope[" + entity.getType().getName() + "]";
		}
	}

	/**
	 * {@link Scope} for documents shaped by an aggregation stage.
	 *
	 * @param resolver resolves the first segment of a path.
	 * @param parent the scope to fall back to for paths not exposed or {@literal null}.
	 */
	record ExposedFieldsScope(FieldResolver resolver, @Nullable Scope parent) implements Scope {

		public ExposedFieldsScope {
			Assert.notNull(resolver, "FieldResolver must not be null");
		}

		/**
		 * @param name the field name.
		 * @return the {@link Exposure} or {@literal null} if the field is not exposed.
		 */
		public @Nullable Exposure resolve(String name) {
			return resolver.resolve(name);
		}

		@Override
		public String toString() {
			return "ExposedFieldsScope[parent=" + parent + "]";
		}
	}

	/**
	 * Resolves a field name to its {@link Exposure}.
	 */
	@FunctionalInterface
	interface FieldResolver {

		/**
		 * @param name the field name.
		 * @return the {@link Exposure} or {@literal null} if the field is not exposed.
		 */
		@Nullable
		Exposure resolve(String name);
	}

	/**
	 * An exposed field.
	 *
	 * @param mappedName the name to render.
	 * @param source the path the field is a plain reference to, {@literal null} if computed. Segments following the
	 *          exposed field are bound relative to {@code source}.
	 * @param element whether the field holds a single element of {@code source}, e.g. after {@code $unwind}.
	 */
	record Exposure(String mappedName, @Nullable BoundMongoPath source, boolean element) {

		public Exposure {
			Assert.hasLength(mappedName, "Mapped name must not be empty");
			Assert.isTrue(!element || source != null, "Element exposure requires a source");
		}

		/**
		 * @param mappedName the name to render.
		 * @param source the path the field is a plain reference to, must not be {@literal null}.
		 * @return an {@link Exposure} holding the value of {@code source}.
		 */
		public static Exposure reference(String mappedName, BoundMongoPath source) {

			Assert.notNull(source, "Source must not be null");
			return new Exposure(mappedName, source, false);
		}

		/**
		 * @param mappedName the name to render.
		 * @param source the array path the field iterates, must not be {@literal null}.
		 * @return an {@link Exposure} holding a single element of {@code source}.
		 */
		public static Exposure element(String mappedName, BoundMongoPath source) {

			Assert.notNull(source, "Source must not be null");
			return new Exposure(mappedName, source, true);
		}

		/**
		 * @param mappedName the name to render.
		 * @return an {@link Exposure} not backed by a source path.
		 */
		public static Exposure computed(String mappedName) {
			return new Exposure(mappedName, null, false);
		}
	}

	/**
	 * Untyped {@link Scope}. No segment can be resolved.
	 */
	enum NoScope implements Scope {
		INSTANCE
	}
}
