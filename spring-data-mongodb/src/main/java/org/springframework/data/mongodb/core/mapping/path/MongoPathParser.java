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

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.AllPositional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Anchor;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.DbRefField;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Digits;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.FilteredPositional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Name;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Positional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Token;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentLruCache;

/**
 * Tokenizer for MongoDB field paths. Splits on dot ({@literal .}) only and classifies tokens according to the given
 * {@link PathType}. Tokens not allowed by a {@link PathType} are retained as {@link Name names}.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class MongoPathParser {

	/**
	 * Maximum length of a path retained in parser and binder caches. Longer paths are parsed and bound on each use.
	 */
	static final int MAX_CACHEABLE_PATH_LENGTH = 1024;

	private static final ParserRules PROPERTY_RULES = ParserRules.of();
	private static final ParserRules QUERY_RULES = ParserRules.of(TokenRules.POSITIONAL);
	private static final ParserRules UPDATE_RULES = ParserRules.of(TokenRules.POSITIONAL, TokenRules.ARRAY_FILTER);
	private static final ParserRules AGGREGATION_RULES = ParserRules.of(AnchorRules.VARIABLE, AnchorRules.FIELD_REFERENCE,
			TokenRules.DBREF);

	private static final ConcurrentLruCache<CacheKey, MongoPath> CACHE = new ConcurrentLruCache<>(2048,
			key -> doParse(key.path(), key.type()));

	private MongoPathParser() {}

	/**
	 * Parse the given {@code path} as the given {@link PathType}.
	 *
	 * @param path must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return the parsed {@link MongoPath}.
	 * @throws IllegalArgumentException if the path exceeds {@link MongoPath#MAX_SEGMENTS} or is an incomplete field or
	 *           variable reference.
	 */
	static MongoPath parse(String path, PathType type) {

		Assert.notNull(path, "Path must not be null");
		Assert.notNull(type, "PathType must not be null");

		return path.length() > MAX_CACHEABLE_PATH_LENGTH ? doParse(path, type) : CACHE.get(new CacheKey(path, type));
	}

	private static MongoPath doParse(String path, PathType type) {

		ParserRules syntax = syntax(type);
		AnchoredPath anchored = anchor(path, syntax);
		Anchor anchor = anchored.anchor();
		String dotPath = anchored.dotPath();

		if (dotPath == null) {
			return new MongoPath(type, anchor, List.of(), false);
		}

		List<Token> tokens = new ArrayList<>();

		int start = 0;
		int dot;
		do {

			if (tokens.size() == MongoPath.MAX_SEGMENTS) {
				throw new IllegalArgumentException("Path exceeds maximum of %d segments".formatted(MongoPath.MAX_SEGMENTS));
			}

			dot = dotPath.indexOf('.', start);
			String text = dot == -1 ? dotPath.substring(start) : dotPath.substring(start, dot);
			tokens.add(token(text, !tokens.isEmpty() || anchor != Anchor.ROOT, syntax));
			start = dot + 1;
		} while (dot != -1);

		return new MongoPath(type, anchor, tokens, false);
	}

	/**
	 * @return the syntax recognized for the given {@link PathType}.
	 */
	private static ParserRules syntax(PathType type) {

		return switch (type) {
			case PROPERTY -> PROPERTY_RULES;
			case QUERY -> QUERY_RULES;
			case UPDATE -> UPDATE_RULES;
			case AGGREGATION -> AGGREGATION_RULES;
		};
	}

	private static AnchoredPath anchor(String path, ParserRules parserRules) {

		if (path.startsWith("$")) {
			for (AnchorRule rule : parserRules.anchors()) {

				AnchoredPath anchored = rule.tryParse(path);
				if (anchored != null) {
					return anchored;
				}
			}
		}

		return new AnchoredPath(Anchor.ROOT, path);
	}

	private static Token token(String text, boolean nested, ParserRules parserRules) {

		if (nested && text.startsWith("$")) {
			for (TokenRule rule : parserRules.tokens()) {

				Token token = rule.tryParse(text);
				if (token != null) {
					return token;
				}
			}
		}

		return Digits.isDigits(text) ? new Digits(text) : new Name(text);
	}

	private interface Rule<T> {
		/**
		 * @param text the text to recognize, starting with {@code $}.
		 * @return the recognized element or {@literal null}.
		 */
		@Nullable
		T tryParse(String text);
	}

	private interface TokenRule extends Rule<Token> {

	}

	/**
	 * Recognizers for special syntax of nested, {@code $}-prefixed segments.
	 */
	private enum TokenRules implements TokenRule {

		POSITIONAL {

			@Override
			@Nullable
			public Token tryParse(String text) {
				return text.equals("$") ? new Positional() : null;
			}
		},

		ARRAY_FILTER {

			@Override
			@Nullable
			public Token tryParse(String text) {

				if (!text.startsWith("$[") || !text.endsWith("]")) {
					return null;
				}

				String identifier = text.substring(2, text.length() - 1);
				if (identifier.isEmpty()) {
					return new AllPositional();
				}
				return FilteredPositional.isIdentifier(identifier) ? new FilteredPositional(identifier) : null;
			}
		},

		DBREF {

			@Override
			@Nullable
			public Token tryParse(String text) {
				return DbRefField.isDbRefField(text) ? new DbRefField(text) : null;
			}
		};

	}

	private interface AnchorRule extends Rule<AnchoredPath> {

	}

	/**
	 * Recognizers for the anchor of a {@code $}-prefixed path.
	 */
	private enum AnchorRules implements AnchorRule {

		VARIABLE {

			@Override
			@Nullable
			public AnchoredPath tryParse(String path) {

				if (!path.startsWith("$$")) {
					return null;
				}

				int dot = path.indexOf('.');
				String name = dot == -1 ? path.substring(2) : path.substring(2, dot);
				if (name.isEmpty()) {
					throw new IllegalArgumentException("Invalid variable reference '%s'".formatted(path));
				}
				return new AnchoredPath(Anchor.variable(name), dot == -1 ? null : path.substring(dot + 1));
			}
		},

		FIELD_REFERENCE {

			@Override
			@Nullable
			public AnchoredPath tryParse(String path) {

				if (path.startsWith("$$")) {
					return null;
				}

				String remainder = path.substring(1);
				if (remainder.isEmpty()) {
					throw new IllegalArgumentException("Invalid field reference '%s'".formatted(path));
				}
				return new AnchoredPath(Anchor.FIELD_REFERENCE, remainder);
			}
		};
	}

	private record ParserRules(List<AnchorRule> anchors, List<TokenRule> tokens) {

		/**
		 * Partition the given rules by kind, retaining their order of precedence.
		 */
		static ParserRules of(Rule<?>... rules) {

			List<AnchorRule> anchors = new ArrayList<>(rules.length);
			List<TokenRule> tokens = new ArrayList<>(rules.length);

			for (Rule<?> rule : rules) {
				if (rule instanceof AnchorRule anchor) {
					anchors.add(anchor);
				} else if (rule instanceof TokenRule token) {
					tokens.add(token);
				} else {
					throw new IllegalArgumentException("Unsupported rule %s".formatted(rule));
				}
			}

			return new ParserRules(List.copyOf(anchors), List.copyOf(tokens));
		}
	}

	/**
	 * A path split into its {@link Anchor} and the {@link MongoPath#toDotPath() dot path} following it.
	 *
	 * @param dotPath {@literal null} if the anchor is not followed by a dot path, e.g. a bare variable.
	 */
	private record AnchoredPath(Anchor anchor, @Nullable String dotPath) {
	}

	private record CacheKey(String path, PathType type) {
	}
}
