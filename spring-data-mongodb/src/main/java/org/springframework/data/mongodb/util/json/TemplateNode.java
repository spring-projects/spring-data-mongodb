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
package org.springframework.data.mongodb.util.json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonRegularExpression;
import org.bson.BsonType;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Immutable node of a compiled {@link MqlTemplate}. Nodes produce a fresh value for each {@link Binding}.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
sealed interface TemplateNode {

	@Nullable
	Object bind(Binding binding);

	/**
	 * Per invocation binding state.
	 */
	record Binding(ParameterBindingContext context, @Nullable LiteralDecoder decoder) {

		@Nullable
		Object parameter(int index) {
			return context.bindableValueForIndex(index);
		}

		/**
		 * @return binding retaining nested literals as is, for values decoded as a whole.
		 */
		Binding raw() {
			return decoder != null ? new Binding(context, null) : this;
		}
	}

	/**
	 * Decodes literal template values of a given {@link BsonType} using the configured codecs.
	 */
	interface LiteralDecoder {

		/**
		 * @return {@literal true} if nested literals are decoded individually, {@literal false} if the value is passed on
		 *         to a codec as a whole.
		 */
		boolean decodesNested(BsonType type);

		@Nullable
		Object decode(BsonType type, @Nullable Object value);
	}

	/**
	 * An immutable literal value.
	 */
	record Constant(@Nullable Object value) implements TemplateNode {

		@Override
		public @Nullable Object bind(Binding binding) {
			return value;
		}
	}

	/**
	 * Unquoted parameter placeholder ({@code ?0}) retaining the bound value.
	 */
	record Parameter(int index) implements TemplateNode {

		@Override
		public @Nullable Object bind(Binding binding) {
			return binding.parameter(index);
		}
	}

	/**
	 * Value expression. Placeholders are passed on as variables and never rendered into the expression source.
	 */
	record Expression(String expression, List<Variable> variables) implements TemplateNode {

		@Override
		public @Nullable Object bind(Binding binding) {

			Map<String, Object> values = variables.isEmpty() ? Collections.emptyMap() : new HashMap<>(variables.size());
			for (Variable variable : variables) {
				Object value = binding.parameter(variable.index());
				values.put(variable.name(), variable.quoted() ? nullSafeToString(value) : value);
			}
			return binding.context().evaluateExpression(expression, values);
		}

		record Variable(String name, int index, boolean quoted) {
		}
	}

	/**
	 * Quoted string or regular expression pattern consisting of literal text, placeholders and expressions.
	 */
	record StringTemplate(List<Part> parts, boolean escapeParameters, boolean escapeExpressions)
			implements TemplateNode {

		private static final String QUOTE_START = "\\Q";
		private static final String QUOTE_END = "\\E";
		private static final String QUOTE_REPLACEMENT_QUOTE_END = QUOTE_END + "\\" + QUOTE_END + QUOTE_START;

		static boolean isQuoted(String value) {
			return value.contains(QUOTE_START) || value.contains(QUOTE_END);
		}

		@Override
		public String bind(Binding binding) {

			StringBuilder result = new StringBuilder();
			for (Part part : parts) {
				if (part instanceof Text text) {
					result.append(text.value());
				} else if (part instanceof ParameterPart parameter) {
					result.append(escape(nullSafeToString(binding.parameter(parameter.index())), escapeParameters));
				} else if (part instanceof ExpressionPart expression) {
					result.append(escape(nullSafeToString(expression.expression().bind(binding)), escapeExpressions));
				}
			}
			return result.toString();
		}

		private static String escape(String fragment, boolean escape) {
			return escape ? fragment.replace(QUOTE_END, QUOTE_REPLACEMENT_QUOTE_END) : fragment;
		}

		sealed interface Part {
		}

		record Text(String value) implements Part {
		}

		record ParameterPart(int index) implements Part {
		}

		record ExpressionPart(Expression expression) implements Part {
		}
	}

	record RegularExpression(StringTemplate pattern, String options) implements TemplateNode {

		@Override
		public BsonRegularExpression bind(Binding binding) {
			return new BsonRegularExpression(pattern.bind(binding), options);
		}
	}

	record DocumentNode(List<Entry> entries) implements TemplateNode {

		@Override
		public Document bind(Binding binding) {

			Document document = new Document();
			for (Entry entry : entries) {

				Object key = entry.key().bind(binding);
				if (key == null) {
					throw new IllegalArgumentException("Document key must not resolve to null");
				}
				document.put(key.toString(), entry.value().bind(binding));
			}
			return document;
		}

		record Entry(TemplateNode key, TemplateNode value) {
		}
	}

	record ArrayNode(List<TemplateNode> values) implements TemplateNode {

		@Override
		public List<@Nullable Object> bind(Binding binding) {

			List<@Nullable Object> result = new ArrayList<>(values.size());
			for (TemplateNode value : values) {
				result.add(value.bind(binding));
			}
			return result;
		}
	}

	/**
	 * Literal value of the delegate decoded via {@link Binding#decoder()}.
	 */
	record Literal(BsonType type, TemplateNode delegate) implements TemplateNode {

		@Override
		public @Nullable Object bind(Binding binding) {

			LiteralDecoder decoder = binding.decoder();
			if (decoder == null) {
				return delegate.bind(binding);
			}
			return decoder.decode(type, delegate.bind(decoder.decodesNested(type) ? binding : binding.raw()));
		}
	}

	/**
	 * Creates a value (Extended JSON wrapper, shell constructor) from its bound arguments.
	 */
	record Conversion(List<TemplateNode> arguments, Converter converter) implements TemplateNode {

		@Override
		public Object bind(Binding binding) {

			List<@Nullable Object> values = new ArrayList<>(arguments.size());
			for (TemplateNode argument : arguments) {
				values.add(argument.bind(binding));
			}
			return converter.convert(values);
		}

		@FunctionalInterface
		interface Converter {
			Object convert(List<@Nullable Object> arguments);
		}
	}

	static String nullSafeToString(@Nullable Object value) {

		if (value instanceof Date date) {
			return DateTimeFormatter.format(date.getTime());
		}

		return ObjectUtils.nullSafeToString(value);
	}
}
