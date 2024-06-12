/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.function.BiFunction;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link AggregationOperationContext} that combines the available field references from a given
 * {@code AggregationOperationContext} and an {@link FieldsExposingAggregationOperation}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.4
 */
class ExposedFieldsAggregationOperationContext implements AggregationOperationContext {

	private final ExposedFields exposedFields;
	private final AggregationOperationContext rootContext;
	private final FieldLookupPolicy lookupPolicy;
	private final ContextualLookupSupport contextualLookup;

	/**
	 * Creates a new {@link ExposedFieldsAggregationOperationContext} from the given {@link ExposedFields}. Uses the given
	 * {@link AggregationOperationContext} to perform a mapping to mongo types if necessary.
	 *
	 * @param exposedFields must not be {@literal null}.
	 * @param rootContext must not be {@literal null}.
	 * @param lookupPolicy must not be {@literal null}.
	 */
	public ExposedFieldsAggregationOperationContext(ExposedFields exposedFields, AggregationOperationContext rootContext,
			FieldLookupPolicy lookupPolicy) {

		Assert.notNull(exposedFields, "ExposedFields must not be null");
		Assert.notNull(rootContext, "RootContext must not be null");
		Assert.notNull(lookupPolicy, "FieldLookupPolicy must not be null");

		this.exposedFields = exposedFields;
		this.rootContext = rootContext;
		this.lookupPolicy = lookupPolicy;
		this.contextualLookup = ContextualLookupSupport.create(lookupPolicy, this::resolveExposedField, (field, name) -> {
			if (field != null) {
				return new DirectFieldReference(new ExposedField(field, true));
			}
			return new DirectFieldReference(new ExposedField(name, true));
		});
	}

	@Override
	public Document getMappedObject(Document document, @Nullable Class<?> type) {
		return rootContext.getMappedObject(document, type);
	}

	@Override
	public FieldReference getReference(Field field) {

		if (field.isInternal()) {
			return new DirectFieldReference(new ExposedField(field, true));
		}

		return getReference(field, field.getTarget());
	}

	@Override
	public FieldReference getReference(String name) {
		return getReference(null, name);
	}

	@Override
	public Fields getFields(Class<?> type) {
		return rootContext.getFields(type);
	}

	/**
	 * Returns a {@link FieldReference} to the given {@link Field} with the given {@code name}.
	 *
	 * @param field may be {@literal null}.
	 * @param name must not be {@literal null}.
	 * @return
	 */
	protected FieldReference getReference(@Nullable Field field, String name) {

		Assert.notNull(name, "Name must not be null");

		return contextualLookup.get(field, name);
	}

	/**
	 * Resolves a {@link Field}/{@code name} for a {@link FieldReference} if possible.
	 *
	 * @param field may be {@literal null}.
	 * @param name must not be {@literal null}.
	 * @return the resolved reference or {@literal null}.
	 */
	@Nullable
	protected FieldReference resolveExposedField(@Nullable Field field, String name) {

		ExposedField exposedField = exposedFields.getField(name);

		if (exposedField != null) {

			if (field != null) {
				// we return a FieldReference to the given field directly to make sure that we reference the proper alias here.
				return new DirectFieldReference(new ExposedField(field, exposedField.isSynthetic()));
			}

			return new DirectFieldReference(exposedField);
		}

		if (name.contains(".")) {

			// for nested field references we only check that the root field exists.
			ExposedField rootField = exposedFields.getField(name.split("\\.")[0]);

			if (rootField != null) {

				// We have to synthetic to true, in order to render the field-name as is.
				return new DirectFieldReference(new ExposedField(name, true));
			}
		}
		return null;
	}

	/**
	 * @return obtain the root context used to resolve references.
	 * @since 3.1
	 */
	AggregationOperationContext getRootContext() {
		return rootContext;
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return getRootContext().getCodecRegistry();
	}

	@Override
	public AggregationOperationContext continueOnMissingFieldReference() {
		if (!lookupPolicy.isStrict()) {
			return this;
		}
		return new ExposedFieldsAggregationOperationContext(exposedFields, rootContext, FieldLookupPolicy.lenient());
	}

	@Override
	public AggregationOperationContext expose(ExposedFields fields) {
		return new ExposedFieldsAggregationOperationContext(fields, this, lookupPolicy);
	}

	@Override
	public AggregationOperationContext inheritAndExpose(ExposedFields fields) {
		return new InheritingExposedFieldsAggregationOperationContext(fields, this, lookupPolicy);
	}

	static class ContextualLookupSupport {

		private final BiFunction<Field, String, FieldReference> resolver;

		ContextualLookupSupport(BiFunction<Field, String, FieldReference> resolver) {
			this.resolver = resolver;
		}

		public static ContextualLookupSupport create(FieldLookupPolicy lookupPolicy,
				BiFunction<Field, String, FieldReference> resolver, BiFunction<Field, String, FieldReference> fallback) {

			if (lookupPolicy.isStrict()) {
				return new StrictContextualLookup(resolver);
			}

			return new FallbackContextualLookup(resolver, fallback);

		}

		public FieldReference get(@Nullable Field field, String name) {
			return resolver.apply(field, name);
		}
	}

	static class StrictContextualLookup extends ContextualLookupSupport {

		StrictContextualLookup(BiFunction<Field, String, FieldReference> resolver) {
			super(resolver);
		}

		@Override
		@NonNull
		public FieldReference get(Field field, String name) {

			FieldReference lookup = super.get(field, name);

			if (lookup != null) {
				return lookup;
			}

			throw new IllegalArgumentException(String.format("Invalid reference '%s'", name));
		}
	}

	static class FallbackContextualLookup extends ContextualLookupSupport {

		private final BiFunction<Field, String, FieldReference> fallback;

		FallbackContextualLookup(BiFunction<Field, String, FieldReference> resolver,
				BiFunction<Field, String, FieldReference> fallback) {
			super(resolver);
			this.fallback = fallback;
		}

		@Override
		@NonNull
		public FieldReference get(@Nullable Field field, String name) {

			FieldReference lookup = super.get(field, name);

			if (lookup != null) {
				return lookup;
			}

			return fallback.apply(field, name);
		}
	}
}
