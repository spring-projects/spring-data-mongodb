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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.util.Assert;

/**
 * Maps field paths within raw aggregation expressions. Every string value starting with a single {@code $} is a field
 * path, {@code $$<variable>.<path>} refers to a variable. Keys are never mapped and {@code $literal} values are retained
 * as-is. Unresolvable paths are passed through.
 * <p>
 * Variables introduced by {@code $map}, {@code $filter}, {@code $reduce} ({@code $$this}) and {@code $let} are bound
 * against the element respectively the value of their input if the input is a plain field path.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
public final class ExpressionPathMapper {

	private static final String THIS = "this";
	private static final String VALUE = "value";

	private static final Set<String> ROOT_VARIABLES = Set.of("ROOT", "CURRENT");

	private final MongoPathBinder binder;

	/**
	 * Create a new {@link ExpressionPathMapper}.
	 *
	 * @param binder must not be {@literal null}.
	 */
	public ExpressionPathMapper(MongoPathBinder binder) {

		Assert.notNull(binder, "MongoPathBinder must not be null");
		this.binder = binder;
	}

	/**
	 * Map the field paths within the given expression.
	 *
	 * @param expression the expression, typically a {@link Document}.
	 * @param scope must not be {@literal null}.
	 * @return the mapped expression.
	 */
	public @Nullable Object map(@Nullable Object expression, Scope scope) {

		Assert.notNull(scope, "Scope must not be null");
		return map(expression, scope, new Variables());
	}

	private @Nullable Object map(@Nullable Object value, Scope scope, Variables variables) {

		if (value instanceof String string) {
			return mapString(string, scope, variables);
		}

		if (value instanceof Map<?, ?> map) {
			return mapDocument(map, scope, variables);
		}

		if (value instanceof List<?> list) {

			List<Object> result = new ArrayList<>(list.size());
			for (Object element : list) {
				result.add(map(element, scope, variables));
			}
			return result;
		}

		return value;
	}

	private Object mapDocument(Map<?, ?> source, Scope scope, Variables variables) {

		if (source.size() == 1) {

			Map.Entry<?, ?> entry = source.entrySet().iterator().next();
			Object mapped = mapOperator(entry.getKey().toString(), entry.getValue(), source, scope, variables);
			if (mapped != null) {
				return mapped;
			}
		}

		Document result = new Document();
		source.forEach((key, value) -> result.put(key.toString(), map(value, scope, variables)));
		return result;
	}

	/**
	 * @return the mapped operator or {@literal null} if the operator does not introduce variables or field names.
	 */
	private @Nullable Object mapOperator(String operator, @Nullable Object argument, Map<?, ?> source, Scope scope,
			Variables variables) {

		if ("$literal".equals(operator)) {
			return source;
		}

		if (argument instanceof String name) {

			boolean fieldOperator = switch (operator) {
				case "$getField", "$setField", "$unsetField" -> !name.startsWith("$");
				default -> false;
			};
			return fieldOperator ? new Document(operator, fieldName(name, null, scope, variables)) : null;
		}

		if (!(argument instanceof Map<?, ?> arguments)) {
			return null;
		}

		return switch (operator) {
			case "$map", "$filter" -> new Document(operator, mapIteration(arguments, scope, variables, false));
			case "$reduce" -> new Document(operator, mapIteration(arguments, scope, variables, true));
			case "$let" -> new Document(operator, mapLet(arguments, scope, variables));
			case "$getField", "$setField", "$unsetField" ->
				new Document(operator, mapFieldOperator(arguments, scope, variables));
			default -> null;
		};
	}

	/**
	 * {@code input} and {@code initialValue} see the given {@code variables}, the remaining arguments additionally the
	 * iteration variable.
	 *
	 * @param accumulator whether to bind {@code $$value}, shadowing an outer one.
	 */
	private Document mapIteration(Map<?, ?> arguments, Scope scope, Variables variables, boolean accumulator) {

		String variable = accumulator ? THIS : asName(arguments);
		Map<String, Scope.Exposure> bindings = new HashMap<>(4);
		bindings.put(variable, exposure(variable, arguments.get("input"), scope, Scope.Exposure::element));
		if (accumulator) {
			bindings.put(VALUE, Scope.Exposure.computed(VALUE));
		}

		// map outer arguments first, retaining the argument order
		Document result = new Document();
		arguments.forEach((key, value) -> result.put(key.toString(),
				"input".equals(key) || "initialValue".equals(key) ? map(value, scope, variables) : value));

		variables.within(bindings, () -> arguments.forEach((key, value) -> {
			if (!"as".equals(key) && !"input".equals(key) && !"initialValue".equals(key)) {
				result.put(key.toString(), map(value, scope, variables));
			}
		}));
		return result;
	}

	private Document mapFieldOperator(Map<?, ?> arguments, Scope scope, Variables variables) {

		Object input = arguments.get("input");

		Document result = new Document();
		arguments.forEach((key, value) -> {

			if ("field".equals(key) && value instanceof String name && !name.startsWith("$")) {
				result.put("field", fieldName(name, input, scope, variables));
			} else {
				result.put(key.toString(), map(value, scope, variables));
			}
		});
		return result;
	}

	/**
	 * Map a single field name, as used by {@code $getField} and friends, against the given {@code input}. The name is not
	 * split on dots as it addresses one field instead of a path.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param input the input reference the name is resolved within, {@literal null} to use the {@link Scope} itself.
	 * @param scope must not be {@literal null}.
	 * @return the mapped name or {@literal null} if it cannot be resolved, e.g. for {@code $}-prefixed expressions.
	 */
	public @Nullable String mapFieldName(String name, @Nullable String input, Scope scope) {

		Assert.hasLength(name, "Name must not be empty");
		Assert.notNull(scope, "Scope must not be null");

		return name.startsWith("$") ? null : resolveFieldName(name, input, scope, new Variables());
	}

	private String fieldName(String name, @Nullable Object input, Scope scope, Variables variables) {

		String mapped = null;
		if (input == null) {
			mapped = resolveFieldName(name, null, scope, variables);
		} else if (input instanceof String reference) {
			mapped = resolveFieldName(name, reference, scope, variables);
		}

		return mapped != null ? mapped : name;
	}

	private @Nullable String resolveFieldName(String name, @Nullable String input, Scope scope,
			Variables variables) {

		MongoPath path = MongoPath.name(name);
		MongoPath reference = input != null ? reference(input) : null;

		if (input == null || reference != null && isRoot(reference)) {

			BoundMongoPath bound = resolved(bind(path, scope));
			return bound != null ? bound.toMappedPath() : null;
		}

		BoundMongoPath parent = reference != null ? resolved(bindReference(reference, scope, variables)) : null;
		if (parent == null) {
			return null;
		}

		try {

			BoundMongoPath appended = binder.append(parent, path);
			return appended.isResolved() ? appended.toRelativeMappedPath(parent.segments().size()) : null;
		} catch (MappingException | IllegalArgumentException ex) {
			return null;
		}
	}

	private static @Nullable BoundMongoPath resolved(@Nullable BoundMongoPath path) {
		return path != null && path.isResolved() ? path : null;
	}

	private Document mapLet(Map<?, ?> arguments, Scope scope, Variables variables) {

		Map<String, Scope.Exposure> bindings = new HashMap<>();
		Document result = new Document();

		if (arguments.get("vars") instanceof Map<?, ?> vars) {

			Document mappedVars = new Document();
			vars.forEach((name, value) -> {
				mappedVars.put(name.toString(), map(value, scope, variables));
				bindings.put(name.toString(), exposure(name.toString(), value, scope, Scope.Exposure::reference));
			});
			result.put("vars", mappedVars);
		}

		variables.within(bindings, () -> arguments.forEach((key, value) -> {
			if (!"vars".equals(key)) {
				result.put(key.toString(), map(value, scope, variables));
			}
		}));
		return result;
	}

	private Scope.Exposure exposure(String variable, @Nullable Object input, Scope scope,
			BiFunction<String, BoundMongoPath, Scope.Exposure> factory) {

		MongoPath reference = input instanceof String value ? reference(value) : null;

		if (reference != null && reference.anchor() == MongoPath.Anchor.FIELD_REFERENCE) {

			BoundMongoPath source = resolved(bind(reference, scope));
			if (source != null) {
				return factory.apply(variable, source);
			}
		}

		return Scope.Exposure.computed(variable);
	}

	private String mapString(String value, Scope scope, Variables variables) {

		MongoPath reference = reference(value);
		if (reference == null) {
			return value;
		}

		BoundMongoPath bound = bindReference(reference, scope, variables);
		if (bound == null) {
			return value;
		}

		// user variables are bound with the variable name as their first segment
		return isUserVariable(reference) ? "$$" + bound.toMappedPath() : bound.toMappedPath();
	}

	/**
	 * Bind a field path or variable reference. Field paths and {@code $$ROOT}/{@code $$CURRENT} references bind against
	 * the {@link Scope}, other variables against their {@link Scope.Exposure}.
	 *
	 * @return the bound path or {@literal null} for unknown variables and unbindable paths.
	 */
	private @Nullable BoundMongoPath bindReference(MongoPath reference, Scope scope,
			Variables variables) {

		if (!(reference.anchor() instanceof MongoPath.Anchor.Variable variable)
				|| ROOT_VARIABLES.contains(variable.name())) {
			return bind(reference, scope);
		}

		Scope.Exposure exposure = variables.get(variable.name());
		return exposure != null ? bind(reference.variablePath(), Scope.exposing(variable.name(), exposure)) : null;
	}

	/**
	 * @return the value parsed as field path or variable reference or {@literal null} if it is neither.
	 */
	private static @Nullable MongoPath reference(String value) {

		if (!value.startsWith("$")) {
			return null;
		}

		try {
			return MongoPath.parse(value, PathType.AGGREGATION);
		} catch (IllegalArgumentException ex) {
			return null;
		}
	}

	private static boolean isUserVariable(MongoPath reference) {
		return reference.anchor() instanceof MongoPath.Anchor.Variable variable
				&& !ROOT_VARIABLES.contains(variable.name());
	}

	private static boolean isRoot(MongoPath reference) {
		return reference.anchor() instanceof MongoPath.Anchor.Variable variable
				&& ROOT_VARIABLES.contains(variable.name()) && reference.tokens().isEmpty();
	}

	private @Nullable BoundMongoPath bind(MongoPath path, Scope scope) {

		try {
			return binder.bind(path, scope);
		} catch (MappingException | IllegalArgumentException ex) {
			return null;
		}
	}

	private static String asName(Map<?, ?> arguments) {
		return arguments.get("as") instanceof String name ? name : THIS;
	}

	/**
	 * Variables visible at the current position of the traversal. Nested scopes add their bindings on entry and restore
	 * shadowed ones on exit instead of copying the visible bindings.
	 */
	private static final class Variables {

		private final Map<String, Scope.Exposure> bindings = new HashMap<>();

		Scope.@Nullable Exposure get(String name) {
			return bindings.get(name);
		}

		void within(Map<String, Scope.Exposure> scoped, Runnable action) {

			Map<String, Scope.Exposure> shadowed = new HashMap<>();
			scoped.forEach((name, exposure) -> {
				Scope.Exposure previous = bindings.put(name, exposure);
				if (previous != null) {
					shadowed.put(name, previous);
				}
			});

			try {
				action.run();
			} finally {
				scoped.keySet().forEach(name -> {
					Scope.Exposure previous = shadowed.get(name);
					if (previous != null) {
						bindings.put(name, previous);
					} else {
						bindings.remove(name);
					}
				});
			}
		}
	}
}
