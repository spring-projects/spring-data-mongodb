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
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.path.ExpressionPathMapper;
import org.springframework.data.mongodb.core.mapping.path.MongoPathBinder;
import org.springframework.data.mongodb.core.mapping.path.Scope;

/**
 * Access to the {@link Scope} of {@link AggregationOperationContext} chains backed by a
 * {@link TypeBasedAggregationOperationContext}.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
final class AggregationScopes {

	private AggregationScopes() {}

	/**
	 * @return the {@link TypedScope} of the given context or {@literal null} if not backed by a
	 *         {@link TypeBasedAggregationOperationContext}.
	 */
	static @Nullable TypedScope typedScopeOf(AggregationOperationContext context) {

		TypeBasedAggregationOperationContext typeBasedContext = typeBasedContextOf(context);
		if (typeBasedContext == null) {
			return null;
		}

		Scope scope = scopeOf(context);
		return scope != null ? new TypedScope(scope, typeBasedContext) : null;
	}

	private static @Nullable Scope scopeOf(AggregationOperationContext context) {

		if (context instanceof TypeBasedAggregationOperationContext typeBasedContext) {
			return typeBasedContext.getScope();
		}
		if (context instanceof ExposedFieldsAggregationOperationContext exposedFieldsContext) {
			return exposedFieldsContext.getScope();
		}
		if (context instanceof NestedDelegatingExpressionAggregationOperationContext nestedContext) {
			return scopeOf(nestedContext.getDelegate());
		}
		if (context instanceof VariableMappingAggregationOperationContext variableContext) {
			return scopeOf(variableContext.getDelegate());
		}
		return null;
	}

	/**
	 * @return the {@link TypeBasedAggregationOperationContext} backing the given context or {@literal null}.
	 */
	static @Nullable TypeBasedAggregationOperationContext typeBasedContextOf(AggregationOperationContext context) {

		AggregationOperationContext current = context;
		while (true) {
			if (current instanceof ExposedFieldsAggregationOperationContext exposedFieldsContext) {
				current = exposedFieldsContext.getRootContext();
			} else if (current instanceof NestedDelegatingExpressionAggregationOperationContext nestedContext) {
				current = nestedContext.getDelegate();
			} else if (current instanceof VariableMappingAggregationOperationContext variableContext) {
				current = variableContext.getDelegate();
			} else {
				return current instanceof TypeBasedAggregationOperationContext typeBasedContext ? typeBasedContext : null;
			}
		}
	}

	/**
	 * Map field paths within the given raw expression document.
	 *
	 * @see #mapExpression(Object, AggregationOperationContext)
	 */
	static Document mapDocument(Document document, AggregationOperationContext context) {
		return mapExpression(document, context) instanceof Document mapped ? mapped : document;
	}

	/**
	 * Map field paths within the given raw expression against the scope of the given context.
	 *
	 * @return the mapped expression or the expression as-is if the context is not backed by a
	 *         {@link TypeBasedAggregationOperationContext}.
	 */
	static @Nullable Object mapExpression(@Nullable Object expression, AggregationOperationContext context) {

		TypedScope typedScope = typedScopeOf(context);
		return typedScope != null ? typedScope.expressionMapper().map(expression, typedScope.scope()) : expression;
	}

	/**
	 * Map a single field name, as used by {@code $getField} and friends, against the given {@code input}.
	 *
	 * @return the mapped name or {@literal null} if it cannot be resolved.
	 */
	static @Nullable String mapFieldName(String name, @Nullable Object input, AggregationOperationContext context) {

		TypedScope typedScope = typedScopeOf(context);

		if (typedScope == null) {
			return null;
		}

		String reference;
		if (input == null || input instanceof AggregationVariable) {
			reference = input == null ? null : input.toString();
		} else if (input instanceof Field field) {
			reference = "$" + field.getTarget();
		} else if (input instanceof String string) {
			reference = string;
		} else {
			return null;
		}

		return typedScope.expressionMapper().mapFieldName(name, reference, typedScope.scope());
	}

	/**
	 * A {@link Scope} together with the {@link TypeBasedAggregationOperationContext} backing it.
	 */
	record TypedScope(Scope scope, TypeBasedAggregationOperationContext typeBasedContext) {

		MongoPathBinder binder() {
			return MongoPathBinder.of(typeBasedContext.getMappingContext());
		}

		ExpressionPathMapper expressionMapper() {
			return new ExpressionPathMapper(binder());
		}
	}
}
