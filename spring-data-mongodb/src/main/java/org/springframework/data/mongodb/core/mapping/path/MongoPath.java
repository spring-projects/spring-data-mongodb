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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.data.core.PropertyPath;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A parsed, syntactic MongoDB field path consisting of an {@link Anchor} and a sequence of {@link Token tokens}, along
 * with the {@link PathType} it was created for. A {@link MongoPath} has no knowledge of the domain model. Use
 * {@link MongoPathBinder} to resolve it against a {@link Scope}.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
public final class MongoPath {

	/**
	 * Maximum number of segments a path may consist of.
	 */
	public static final int MAX_SEGMENTS = 100;

	private final PathType type;
	private final Anchor anchor;
	private final List<Token> tokens;
	private final boolean propertyReference;
	private final int length;

	MongoPath(PathType type, Anchor anchor, List<Token> tokens, boolean propertyReference) {

		this.type = type;
		this.anchor = anchor;
		this.tokens = List.copyOf(tokens);
		this.propertyReference = propertyReference;

		int length = anchor.render("").length() + Math.max(0, tokens.size() - 1);
		for (Token token : tokens) {
			length += token.text().length();
		}
		this.length = length;
	}

	/**
	 * Parse the given {@code path} as the given {@link PathType}.
	 *
	 * @param path must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return the parsed {@link MongoPath}.
	 * @throws IllegalArgumentException if the path exceeds {@link #MAX_SEGMENTS} or is an incomplete field or variable
	 *           reference such as {@code $} or {@code $$}.
	 */
	public static MongoPath parse(String path, PathType type) {
		return MongoPathParser.parse(path, type);
	}

	/**
	 * Create a {@link MongoPath} consisting of a single name that is not split on dots. Used for field names of operators
	 * such as {@code $getField} that address one field instead of a path.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return new instance of {@link MongoPath}.
	 */
	public static MongoPath name(String name) {

		Assert.hasLength(name, "Name must not be empty");
		return new MongoPath(PathType.PROPERTY, Anchor.ROOT, List.of(new Name(name)), false);
	}

	/**
	 * Create a {@link PathType#PROPERTY} {@link MongoPath} from a {@link PropertyPath}. Segments of the resulting path
	 * refer to Java property names only.
	 *
	 * @param path must not be {@literal null}.
	 * @return new instance of {@link MongoPath}.
	 */
	public static MongoPath from(PropertyPath path) {

		Assert.notNull(path, "PropertyPath must not be null");

		List<Token> tokens = new ArrayList<>();
		for (PropertyPath segment : path) {
			tokens.add(new Name(segment.getSegment()));
		}
		return new MongoPath(PathType.PROPERTY, Anchor.ROOT, tokens, true);
	}

	/**
	 * @return the {@link PathType} this path was created for.
	 */
	public PathType type() {
		return type;
	}

	/**
	 * @return the {@link Anchor} of this path.
	 */
	public Anchor anchor() {
		return anchor;
	}

	/**
	 * @return the {@link Token tokens} following the {@link #anchor()}.
	 */
	public List<Token> tokens() {
		return tokens;
	}

	/**
	 * @return {@literal true} if the path originates from a {@link PropertyPath} and must be resolved by Java property
	 *         names only.
	 */
	public boolean isPropertyReference() {
		return propertyReference;
	}

	/**
	 * Append the given {@code path} to this one. The result keeps the {@link #type()} of this path.
	 *
	 * @param path must not be {@literal null}, must be anchored at {@link Anchor#ROOT} and either be of the same
	 *          {@link PathType} or a {@link PathType#PROPERTY} path.
	 * @return new instance of {@link MongoPath}.
	 */
	public MongoPath append(MongoPath path) {

		Assert.notNull(path, "Path must not be null");
		Assert.isTrue(path.anchor == Anchor.ROOT, "Appended path must not have an anchor");
		Assert.isTrue(path.type == type || path.type == PathType.PROPERTY,
				() -> "Cannot append %s path '%s' to %s path '%s'".formatted(path.type, path, type, this));

		List<Token> tokens = new ArrayList<>(this.tokens.size() + path.tokens.size());
		tokens.addAll(this.tokens);
		tokens.addAll(path.tokens);
		return new MongoPath(type, anchor, tokens, propertyReference && path.propertyReference);
	}

	/**
	 * @return the path without its first token, anchored at the root.
	 */
	MongoPath tail() {

		Assert.state(!tokens.isEmpty(), "Path has no tokens");
		return new MongoPath(type, Anchor.ROOT, tokens.subList(1, tokens.size()), propertyReference);
	}

	/**
	 * @return the path of a {@link Anchor.Variable variable}, anchored at the root with the variable name as its first
	 *         token.
	 */
	MongoPath variablePath() {

		Assert.state(anchor instanceof Anchor.Variable, "Path is not anchored at a variable");

		List<Token> tokens = new ArrayList<>(this.tokens.size() + 1);
		tokens.add(new Name(((Anchor.Variable) anchor).name()));
		tokens.addAll(this.tokens);
		return new MongoPath(type, Anchor.ROOT, tokens, propertyReference);
	}

	/**
	 * @return the approximate length of the path including the {@link #anchor()}.
	 */
	int length() {
		return length;
	}

	/**
	 * @return the dot path without the {@link #anchor()}.
	 */
	public String toDotPath() {
		return tokens.stream().map(Token::text).collect(Collectors.joining("."));
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof MongoPath that)) {
			return false;
		}
		return type == that.type && propertyReference == that.propertyReference && anchor.equals(that.anchor)
				&& tokens.equals(that.tokens);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, anchor, tokens, propertyReference);
	}

	@Override
	public String toString() {
		return anchor.render(toDotPath());
	}

	/**
	 * The starting point of a {@link MongoPath}.
	 */
	public interface Anchor {

		/**
		 * The current document root (no prefix).
		 */
		Anchor ROOT = Root.INSTANCE;

		/**
		 * A field reference using the {@code $} prefix.
		 */
		Anchor FIELD_REFERENCE = FieldReference.INSTANCE;

		/**
		 * @param name the variable name without the {@code $$} prefix.
		 * @return new {@link Variable}.
		 */
		static Anchor variable(String name) {
			return new Variable(name);
		}

		/**
		 * Render the given dot path including the anchor prefix.
		 *
		 * @param dotPath must not be {@literal null}.
		 * @return the rendered path.
		 */
		String render(String dotPath);

		enum Root implements Anchor {

			INSTANCE;

			@Override
			public String render(String dotPath) {
				return dotPath;
			}
		}

		enum FieldReference implements Anchor {

			INSTANCE;

			@Override
			public String render(String dotPath) {
				return "$" + dotPath;
			}
		}

		/**
		 * A variable such as {@code $$this}, {@code $$ROOT} or a user defined one.
		 *
		 * @param name the variable name without the {@code $$} prefix.
		 */
		record Variable(String name) implements Anchor {

			public Variable {
				Assert.hasText(name, "Variable name must not be empty");
			}

			@Override
			public String render(String dotPath) {
				return StringUtils.hasLength(dotPath) ? "$$" + name + "." + dotPath : "$$" + name;
			}
		}
	}

	/**
	 * A syntactic path element.
	 */
	public interface Token {

		/**
		 * @return the raw text of the token.
		 */
		String text();
	}

	/**
	 * A name, either a property or a field name.
	 *
	 * @param text the raw name.
	 */
	public record Name(String text) implements Token {
	}

	/**
	 * A purely numeric token. Interpretation (list index, map key, field name) depends on the bound type.
	 *
	 * @param text the raw digits, retaining leading zeros.
	 */
	public record Digits(String text) implements Token {

		public Digits {
			Assert.isTrue(isDigits(text), "Not a number");
		}

		static boolean isDigits(String text) {

			if (text.isEmpty()) {
				return false;
			}
			for (int i = 0; i < text.length(); i++) {
				char c = text.charAt(i);
				if (c < '0' || c > '9') {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * A {@code $id}, {@code $ref} or {@code $db} field of a {@link com.mongodb.DBRef} in aggregation paths such as
	 * {@code person.$id}. Treated as a {@link Name} if not following a
	 * {@link org.springframework.data.mongodb.core.mapping.DBRef} property.
	 *
	 * @param text the raw field name.
	 */
	public record DbRefField(String text) implements Token {

		public DbRefField {
			Assert.isTrue(isDbRefField(text), "Not a DBRef field");
		}

		static boolean isDbRefField(String text) {
			return "$id".equals(text) || "$ref".equals(text) || "$db".equals(text);
		}
	}

	/**
	 * Positional update or projection operators.
	 */
	public interface PositionalToken extends Token {}

	/**
	 * The {@code $} positional operator.
	 */
	public record Positional() implements PositionalToken {

		@Override
		public String text() {
			return "$";
		}
	}

	/**
	 * The {@code $[]} all positional operator.
	 */
	public record AllPositional() implements PositionalToken {

		@Override
		public String text() {
			return "$[]";
		}
	}

	/**
	 * The {@code $[<identifier>]} filtered positional operator.
	 *
	 * @param identifier the array filter identifier.
	 */
	public record FilteredPositional(String identifier) implements PositionalToken {

		public FilteredPositional {
			Assert.isTrue(isIdentifier(identifier), "Invalid array filter identifier");
		}

		/**
		 * Lenient on purpose: identifier validation is left to the server.
		 */
		static boolean isIdentifier(String text) {
			return !text.isEmpty() && text.indexOf('[') == -1 && text.indexOf(']') == -1;
		}

		@Override
		public String text() {
			return "$[" + identifier + "]";
		}
	}

	/**
	 * What a {@link MongoPath} is created for, defining which tokens are recognized.
	 */
	public enum PathType {

		/**
		 * A plain dotted path within a type, e.g. index keys or schema properties. Recognizes no operators.
		 */
		PROPERTY,

		/**
		 * Query filters, sort and projections. Recognizes the {@code $} positional operator.
		 */
		QUERY,

		/**
		 * Update documents. Recognizes {@code $}, {@code $[]} and {@code $[<identifier>]}.
		 */
		UPDATE,

		/**
		 * Aggregation pipelines. A leading {@code $} denotes a field reference, a leading {@code $$} a variable. Positional
		 * operators are not recognized, a nested {@code $id}, {@code $ref} or {@code $db} is a field of a DBRef.
		 */
		AGGREGATION
	}
}
