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
import java.util.List;
import java.util.Map;

import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mongodb.util.json.TemplateNode.Binding;
import org.springframework.data.mongodb.util.json.TemplateNode.DocumentNode;
import org.springframework.data.mongodb.util.json.TemplateNode.LiteralDecoder;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentLruCache;

/**
 * Compiled JSON template parsed once into an immutable structure.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class MqlTemplate {

	private static final ConcurrentLruCache<String, MqlTemplate> CACHE = new ConcurrentLruCache<>(256,
			MqlTemplateCompiler::parse);

	private final TemplateNode root;
	private final List<TemplateNode.Expression> expressions;

	/**
	 * @param root either a {@link DocumentNode} or a {@link TemplateNode.Parameter} or {@link TemplateNode.Expression}
	 *          binding the entire document.
	 * @param expressions all expressions contained in the template.
	 */
	MqlTemplate(TemplateNode root, List<TemplateNode.Expression> expressions) {

		this.root = root;
		this.expressions = List.copyOf(expressions);
	}

	/**
	 * Parse the given {@code source} or obtain a previously parsed template.
	 *
	 * @param source must not be {@literal null}.
	 * @return the compiled template.
	 * @throws JsonParseException if the source is not a valid template.
	 */
	static MqlTemplate parse(String source) {
		return CACHE.get(source);
	}

	/**
	 * Bind parameters and evaluate expressions.
	 *
	 * @param context the binding context.
	 * @param decoder applied to literal nested documents and {@code undefined}.
	 * @return a new {@link Document}.
	 */
	@SuppressWarnings("unchecked")
	Document bind(ParameterBindingContext context, LiteralDecoder decoder) {

		Binding binding = new Binding(context, decoder);

		if (root instanceof DocumentNode document) {
			return document.bind(binding);
		}

		Object value = root.bind(binding);

		if (value instanceof Document document) {
			return document;
		}
		if (value instanceof String json) {
			try {
				return Document.parse(json);
			} catch (JsonParseException | BsonInvalidOperationException e) {
				throw new IllegalArgumentException("Expression result is not a valid json document!", e);
			}
		}
		if (value instanceof Map<?, ?> map) {
			return new Document((Map<String, Object>) map);
		}
		if (value == null) {
			return new Document();
		}

		throw new IllegalArgumentException(
				"Binding result of type %s is not a document".formatted(ClassUtils.getQualifiedName(value.getClass())));
	}

	/**
	 * Determine {@link ExpressionDependencies} without binding parameters or evaluating expressions.
	 */
	ExpressionDependencies getExpressionDependencies(ValueExpressionParser parser) {

		if (expressions.isEmpty()) {
			return ExpressionDependencies.none();
		}

		List<ExpressionDependencies> dependencies = new ArrayList<>(expressions.size());
		for (TemplateNode.Expression expression : expressions) {
			dependencies.add(parser.parse(expression.expression()).getExpressionDependencies());
		}
		return ExpressionDependencies.merged(dependencies);
	}
}
