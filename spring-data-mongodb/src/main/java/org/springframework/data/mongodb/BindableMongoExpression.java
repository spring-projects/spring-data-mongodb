/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb;

import java.util.Arrays;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link MongoExpression} using the {@link ParameterBindingDocumentCodec} for parsing a raw ({@literal json})
 * expression. The expression will be wrapped within <code>{ ... }</code> if necessary. The actual parsing and parameter
 * binding of placeholders like {@code ?0} is delayed upon first call on the the target {@link Document} via
 * {@link #toDocument()}.
 * <br />
 *
 * <pre class="code">
 * $toUpper : $name                -> { '$toUpper' : '$name' }
 *
 * { '$toUpper' : '$name' }        -> { '$toUpper' : '$name' }
 *
 * { '$toUpper' : '?0' }, "$name"  -> { '$toUpper' : '$name' }
 * </pre>
 *
 * Some types might require a special {@link org.bson.codecs.Codec}. If so, make sure to provide a {@link CodecRegistry}
 * containing the required {@link org.bson.codecs.Codec codec} via {@link #withCodecRegistry(CodecRegistry)}.
 *
 * @author Christoph Strobl
 * @since 3.2
 */
public class BindableMongoExpression implements MongoExpression {

	private final String expressionString;

	private final @Nullable CodecRegistryProvider codecRegistryProvider;

	private final @Nullable Object[] args;

	private final Lazy<Document> target;

	/**
	 * Create a new instance of {@link BindableMongoExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param args can be {@literal null}.
	 */
	public BindableMongoExpression(String expression, @Nullable Object[] args) {
		this(expression, null, args);
	}

	/**
	 * Create a new instance of {@link BindableMongoExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param codecRegistryProvider can be {@literal null}.
	 * @param args can be {@literal null}.
	 */
	public BindableMongoExpression(String expression, @Nullable CodecRegistryProvider codecRegistryProvider,
			@Nullable Object[] args) {

		this.expressionString = expression;
		this.codecRegistryProvider = codecRegistryProvider;
		this.args = args;
		this.target = Lazy.of(this::parse);
	}

	/**
	 * Provide the {@link CodecRegistry} used to convert expressions.
	 *
	 * @param codecRegistry must not be {@literal null}.
	 * @return new instance of {@link BindableMongoExpression}.
	 */
	public BindableMongoExpression withCodecRegistry(CodecRegistry codecRegistry) {
		return new BindableMongoExpression(expressionString, () -> codecRegistry, args);
	}

	/**
	 * Provide the arguments to bind to the placeholders via their index.
	 *
	 * @param args must not be {@literal null}.
	 * @return new instance of {@link BindableMongoExpression}.
	 */
	public BindableMongoExpression bind(Object... args) {
		return new BindableMongoExpression(expressionString, codecRegistryProvider, args);
	}

	@Override
	public Document toDocument() {
		return target.get();
	}

	@Override
	public String toString() {
		return "BindableMongoExpression{" + "expressionString='" + expressionString + '\'' + ", args="
				+ Arrays.toString(args) + '}';
	}

	private Document parse() {

		String expression = wrapJsonIfNecessary(expressionString);

		if (ObjectUtils.isEmpty(args)) {

			if (codecRegistryProvider == null) {
				return Document.parse(expression);
			}

			return Document.parse(expression, codecRegistryProvider.getCodecFor(Document.class)
					.orElseGet(() -> new DocumentCodec(codecRegistryProvider.getCodecRegistry())));
		}

		ParameterBindingDocumentCodec codec = codecRegistryProvider == null ? new ParameterBindingDocumentCodec()
				: new ParameterBindingDocumentCodec(codecRegistryProvider.getCodecRegistry());
		return codec.decode(expression, args);
	}

	private static String wrapJsonIfNecessary(String json) {

		if (StringUtils.hasText(json) && (json.startsWith("{") && json.endsWith("}"))) {
			return json;
		}

		return "{" + json + "}";
	}
}
