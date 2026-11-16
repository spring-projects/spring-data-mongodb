/*
 * Copyright 2013-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.aggregation.AggregationScopes.TypedScope;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DeclaredReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath;
import org.springframework.data.mongodb.core.mapping.path.MongoPathBinder;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.Scope;
import org.springframework.data.util.Lazy;
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
	private final Lazy<@Nullable Scope> scope = Lazy.of(this::createScope);

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
	}

	@Override
	public Document getMappedObject(Document document, @Nullable Class<?> type) {
		return rootContext.getMappedObject(document, type);
	}

	@Override
	public Document getMappedCriteria(Document criteria) {

		TypedScope typedScope = AggregationScopes.typedScopeOf(this);

		if (typedScope == null) {
			return getMappedObject(criteria);
		}

		return typedScope.typeBasedContext().getQueryMapper().getMappedObject((Document) renderExpressions(criteria),
				typedScope.scope());
	}

	/**
	 * Render typed {@link AggregationExpression}s (e.g. {@code $expr}) against this context, as the
	 * {@link org.springframework.data.mongodb.core.convert.QueryMapper} only knows the {@link Scope}.
	 */
	private Object renderExpressions(Object value) {

		if (value instanceof AggregationExpression expression) {
			return expression.toDocument(this);
		}

		if (value instanceof Document document) {

			Document result = new Document();
			document.forEach((key, nested) -> result.put(key, renderExpressions(nested)));
			return result;
		}

		if (value instanceof List<?> list) {

			List<Object> result = new ArrayList<>(list.size());
			list.forEach(element -> result.add(renderExpressions(element)));
			return result;
		}

		return value;
	}

	/**
	 * @return the {@link Scope} describing the document shape exposed by this context or {@literal null} if the context
	 *         chain is not backed by a {@link TypeBasedAggregationOperationContext}.
	 */
	@Nullable
	Scope getScope() {
		return scope.getNullable();
	}

	/**
	 * @return whether fields not exposed by this context fall back to the previous context.
	 */
	boolean inheritsFields() {
		return false;
	}

	private @Nullable Scope createScope() {

		TypedScope parent = AggregationScopes.typedScopeOf(rootContext);

		if (parent == null) {
			return null;
		}

		Scope parentScope = parent.scope();
		MongoPathBinder binder = parent.binder();
		return Scope.exposing(name -> resolve(name, parentScope, binder), inheritsFields() ? parentScope : null);
	}

	private Scope.@Nullable Exposure resolve(String name, Scope parentScope, MongoPathBinder binder) {

		ExposedField field = exposedFields.getField(name);
		DeclaredReference reference = exposedFields.getDeclaredReference(name);

		if (field == null && reference == null) {
			return null;
		}

		// undeclared fields of stages inheriting fields map against the input
		if (inheritsFields() && !exposedFields.isDeclared(name)) {
			return null;
		}

		boolean element = reference != null && reference.element();
		BoundMongoPath source = null;
		if (reference != null) {
			try {
				BoundMongoPath bound = binder.bind(reference.target(), PathType.AGGREGATION, parentScope);
				source = bound.isResolved() ? bound : null;
			} catch (MappingException | IllegalArgumentException ex) {
				// not resolvable, treat as computed
			}
		}

		if (field == null) {
			// e.g. $unwind which does not expose the unwound field
			return source != null ? new Scope.Exposure(source.toMappedPath(), source, element) : null;
		}

		String mappedName;
		if (!field.isSynthetic()) {
			mappedName = exposedFields.exposesSingleNonSyntheticFieldOnly() ? Fields.UNDERSCORE_ID
					: Fields.UNDERSCORE_ID + "." + field.getName();
		} else if (exposedFields.isInputMappedName(name)) {
			mappedName = mapAgainstInput(name, binder);
		} else {
			mappedName = field.getName();
		}

		return new Scope.Exposure(mappedName, source, source != null && element);
	}

	private String mapNestedReference(String name) {

		TypedScope typedScope = AggregationScopes.typedScopeOf(this);

		if (typedScope == null) {
			return name;
		}

		try {
			return typedScope.binder().bind(name, PathType.AGGREGATION, typedScope.scope()).toMappedPath();
		} catch (MappingException | IllegalArgumentException ex) {
			return name;
		}
	}

	private String mapAgainstInput(String name, MongoPathBinder binder) {

		try {
			return binder.bind(name, PathType.QUERY, getDocumentMappingScope(rootContext)).toMappedPath();
		} catch (MappingException | IllegalArgumentException ex) {
			return name;
		}
	}

	/**
	 * @return the {@link Scope} {@link #getMappedObject(Document)} of the given context maps document keys against.
	 */
	private static Scope getDocumentMappingScope(AggregationOperationContext context) {

		if (context instanceof TypeBasedAggregationOperationContext typeBasedContext) {
			return typeBasedContext.getScope();
		}
		if (context instanceof InheritingExposedFieldsAggregationOperationContext inheritingContext) {
			return getDocumentMappingScope(inheritingContext.getRootContext());
		}
		return Scope.none();
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
	private FieldReference getReference(@Nullable Field field, String name) {

		Assert.notNull(name, "Name must not be null");

		FieldReference exposedField = resolveExposedField(field, name);
		if (exposedField != null) {
			return exposedField;
		}

		if (lookupPolicy.isStrict()) {
			throw new IllegalArgumentException(String.format("Invalid reference '%s'", name));
		}

		if (field != null) {
			return new DirectFieldReference(new ExposedField(field, true));
		}

		return new DirectFieldReference(new ExposedField(name, true));
	}

	/**
	 * Resolves a {@link Field}/{@code name} for a {@link FieldReference} if possible.
	 *
	 * @param field may be {@literal null}.
	 * @param name must not be {@literal null}.
	 * @return the resolved reference or {@literal null}.
	 */
	protected @Nullable FieldReference resolveExposedField(@Nullable Field field, String name) {

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
				return new DirectFieldReference(new ExposedField(mapNestedReference(name), true));
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
	@Deprecated(since = "4.3.1", forRemoval = true)
	public AggregationOperationContext continueOnMissingFieldReference() {
		if (!lookupPolicy.isStrict()) {
			return this;
		}
		return new ExposedFieldsAggregationOperationContext(exposedFields, rootContext, FieldLookupPolicy.relaxed());
	}

	@Override
	public AggregationOperationContext expose(ExposedFields fields) {
		return new ExposedFieldsAggregationOperationContext(fields, this, lookupPolicy);
	}

	@Override
	public AggregationOperationContext inheritAndExpose(ExposedFields fields) {
		return new InheritingExposedFieldsAggregationOperationContext(fields, this, lookupPolicy);
	}

}
