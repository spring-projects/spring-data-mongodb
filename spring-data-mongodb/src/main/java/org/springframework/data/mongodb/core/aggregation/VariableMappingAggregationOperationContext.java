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
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.aggregation.AggregationScopes.TypedScope;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath;
import org.springframework.data.mongodb.core.mapping.path.MongoPathBinder;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.Scope;

/**
 * {@link AggregationOperationContext} mapping references to {@code $$<variable>.<path>} against the element type of the
 * array the variable iterates. Other references are resolved by the delegate.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class VariableMappingAggregationOperationContext implements AggregationOperationContext {

	private final AggregationOperationContext delegate;
	private final String prefix;
	private final Scope variableScope;
	private final MongoPathBinder binder;

	private VariableMappingAggregationOperationContext(AggregationOperationContext delegate, String variable,
			Scope variableScope, MongoPathBinder binder) {

		this.delegate = delegate;
		this.prefix = "$$" + variable + ".";
		this.variableScope = variableScope;
		this.binder = binder;
	}

	/**
	 * Create a context mapping {@code $$<variable>.<path>} references against the elements of {@code input}.
	 *
	 * @return the mapping context or {@code delegate} if {@code input} cannot be resolved.
	 */
	static AggregationOperationContext forElementsOf(AggregationOperationContext delegate, String variable,
			@Nullable Object input) {

		if (!(input instanceof Field field)) {
			return delegate;
		}

		TypedScope typedScope = AggregationScopes.typedScopeOf(delegate);

		if (typedScope == null) {
			return delegate;
		}

		MongoPathBinder binder = typedScope.binder();

		BoundMongoPath source;
		try {
			source = binder.bind(field.getTarget(), PathType.AGGREGATION, typedScope.scope());
		} catch (MappingException | IllegalArgumentException ex) {
			return delegate;
		}

		if (!source.isResolved()) {
			return delegate;
		}

		Scope variableScope = Scope.exposing(variable, Scope.Exposure.element(variable, source));

		return new VariableMappingAggregationOperationContext(delegate, variable, variableScope, binder);
	}

	@Override
	public FieldReference getReference(Field field) {

		String target = field.getTarget();
		if (!target.startsWith(prefix)) {
			return delegate.getReference(field);
		}

		try {
			String mapped = binder.bind(target.substring(2), PathType.AGGREGATION, variableScope)
					.toMappedPath();
			return new DirectFieldReference(new ExposedField(Fields.field("$$" + mapped), true));
		} catch (MappingException | IllegalArgumentException ex) {
			return delegate.getReference(field);
		}
	}

	@Override
	public FieldReference getReference(String name) {
		return delegate.getReference(name);
	}

	@Override
	public Document getMappedObject(Document document) {
		return delegate.getMappedObject(document);
	}

	@Override
	public Document getMappedObject(Document document, @Nullable Class<?> type) {
		return delegate.getMappedObject(document, type);
	}

	@Override
	public Document getMappedCriteria(Document criteria) {
		return delegate.getMappedCriteria(criteria);
	}

	@Override
	public Fields getFields(Class<?> type) {
		return delegate.getFields(type);
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return delegate.getCodecRegistry();
	}

	AggregationOperationContext getDelegate() {
		return delegate;
	}
}
