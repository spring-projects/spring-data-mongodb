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

import static java.lang.String.*;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDouble;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.util.json.TemplateNode.ArrayNode;
import org.springframework.data.mongodb.util.json.TemplateNode.Constant;
import org.springframework.data.mongodb.util.json.TemplateNode.Conversion;
import org.springframework.data.mongodb.util.json.TemplateNode.DocumentNode;
import org.springframework.data.mongodb.util.json.TemplateNode.Expression;
import org.springframework.data.mongodb.util.json.TemplateNode.Expression.Variable;
import org.springframework.data.mongodb.util.json.TemplateNode.Literal;
import org.springframework.data.mongodb.util.json.TemplateNode.Parameter;
import org.springframework.data.mongodb.util.json.TemplateNode.RegularExpression;
import org.springframework.data.mongodb.util.json.TemplateNode.StringTemplate;
import org.springframework.data.mongodb.util.json.TemplateNode.StringTemplate.ExpressionPart;
import org.springframework.data.mongodb.util.json.TemplateNode.StringTemplate.ParameterPart;
import org.springframework.data.mongodb.util.json.TemplateNode.StringTemplate.Part;
import org.springframework.data.mongodb.util.json.TemplateNode.StringTemplate.Text;
import org.springframework.data.mongodb.util.json.MqlTemplateParser.*;
import org.springframework.util.NumberUtils;

/**
 * Compiles a JSON template into an immutable {@link TemplateNode} tree. Syntax and the structure of Extended JSON
 * wrappers and shell constructors are validated before any parameter is bound or expression is evaluated.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class MqlTemplateCompiler extends MqlTemplateBaseVisitor<TemplateNode> {

	static final int MAX_NESTING_DEPTH = 256;

	private static final Pattern PLACEHOLDER_INDEX = Pattern.compile("^\\?(\\d+)");
	private static final String DATE_FORMAT = "EEE MMM dd yyyy HH:mm:ss z";
	private static final Set<String> CONSTRUCTORS = Set.of("BinData", "Date", "DBPointer", "HexData", "ISODate",
			"NumberDecimal", "NumberInt", "NumberLong", "ObjectId", "RegExp", "Timestamp", "UUID");

	private final List<Expression> expressions = new ArrayList<>();

	private MqlTemplateCompiler() {}

	static MqlTemplate parse(String source) {

		ValueContext root = doParse(source);
		MqlTemplateCompiler compiler = new MqlTemplateCompiler();

		if (root instanceof ObjectValueContext object) {
			return new MqlTemplate(compiler.document(object.object().entry()), compiler.expressions);
		}

		TemplateNode node = compiler.visit(root);
		if (node instanceof Parameter || node instanceof Expression) {
			return new MqlTemplate(node, compiler.expressions);
		}

		throw new JsonParseException("JSON reader expected a document, parameter or expression but found '%s'.",
				root.getText());
	}

	private static ValueContext doParse(String source) {

		MqlTemplateLexer lexer = new MqlTemplateLexer(CharStreams.fromString(source));
		lexer.removeErrorListeners();
		lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

		CommonTokenStream tokens = new CommonTokenStream(lexer);
		MqlTemplateParser parser = new MqlTemplateParser(tokens);
		parser.removeErrorListeners();
		parser.setErrorHandler(new BailErrorStrategy());
		parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
		parser.addParseListener(new NestingDepthLimit());

		try {
			return parser.template().value();
		} catch (ParseCancellationException ex) {

			// retry with full LL prediction and report the actual syntax error
			tokens.seek(0);
			parser.reset();
			parser.removeParseListeners();
			parser.addParseListener(new NestingDepthLimit());
			parser.addErrorListener(ThrowingErrorListener.INSTANCE);
			parser.setErrorHandler(new DefaultErrorStrategy());
			parser.getInterpreter().setPredictionMode(PredictionMode.LL);

			return parser.template().value();
		}
	}

	@Override
	public TemplateNode visitObjectValue(ObjectValueContext ctx) {
		return object(ctx.object().entry());
	}

	@Override
	public TemplateNode visitArrayValue(ArrayValueContext ctx) {

		List<TemplateNode> values = new ArrayList<>();
		for (ValueContext value : ctx.array().value()) {
			values.add(visit(value));
		}
		return new ArrayNode(List.copyOf(values));
	}

	@Override
	public TemplateNode visitStringValue(StringValueContext ctx) {
		return string(unquote(ctx.STRING().getText()));
	}

	@Override
	public TemplateNode visitRegexValue(RegexValueContext ctx) {
		return regex(ctx.REGEX().getText());
	}

	@Override
	public TemplateNode visitNumberValue(NumberValueContext ctx) {
		return new Constant(number(ctx.NUMBER().getText()));
	}

	@Override
	public TemplateNode visitPlaceholderValue(PlaceholderValueContext ctx) {
		return placeholder(ctx.PLACEHOLDER().getText());
	}

	@Override
	public TemplateNode visitExpressionValue(ExpressionValueContext ctx) {
		return expression(ctx.EXPRESSION().getText());
	}

	@Override
	public TemplateNode visitNewConstructorValue(NewConstructorValueContext ctx) {
		return constructor(ctx.NAME().getText(), ctx.arguments(), true);
	}

	@Override
	public TemplateNode visitConstructorValue(ConstructorValueContext ctx) {
		return constructor(ctx.NAME().getText(), ctx.arguments(), false);
	}

	@Override
	public TemplateNode visitNameValue(NameValueContext ctx) {
		return name(ctx.NAME().getText());
	}

	@Override
	public TemplateNode visitChildren(RuleNode node) {
		throw new JsonParseException("JSON reader was expecting a value but found '%s'.", node.getText());
	}

	private TemplateNode object(List<EntryContext> entries) {

		if (!entries.isEmpty()) {

			String key = literalKey(entries.get(0).key());
			TemplateNode wrapper = key != null ? wrapper(key, entries) : null;
			if (wrapper != null) {
				return wrapper;
			}
		}

		return new Literal(BsonType.DOCUMENT, document(entries));
	}

	private DocumentNode document(List<EntryContext> entries) {

		List<DocumentNode.Entry> result = new ArrayList<>(entries.size());
		for (EntryContext entry : entries) {
			result.add(new DocumentNode.Entry(key(entry.key()), visit(entry.value())));
		}
		return new DocumentNode(List.copyOf(result));
	}

	private TemplateNode key(KeyContext key) {

		if (key.STRING() != null) {
			return string(unquote(key.STRING().getText()));
		}
		if (key.PLACEHOLDER() != null) {
			return placeholder(key.PLACEHOLDER().getText());
		}
		if (key.EXPRESSION() != null) {
			return expression(key.EXPRESSION().getText());
		}
		return new Constant(key.getText());
	}

	private TemplateNode string(String content) {

		StringTemplate template = template(content, false);
		return template != null ? template : new Constant(content);
	}

	private TemplateNode regex(String token) {

		int end = token.lastIndexOf('/');
		String pattern = token.substring(1, end);
		String options = token.substring(end + 1);

		StringTemplate template = template(pattern, true);
		return template != null ? new RegularExpression(template, options)
				: new Constant(new BsonRegularExpression(pattern, options));
	}

	private @Nullable StringTemplate template(String content, boolean regex) {

		if (content.indexOf('?') == -1 && content.indexOf(':') == -1) {
			return null;
		}

		Lexer lexer = new MqlStringTemplateLexer(CharStreams.fromString(content));
		lexer.removeErrorListeners();

		List<Part> parts = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		boolean bindings = false;

		for (Token token = lexer.nextToken(); token.getType() != Token.EOF; token = lexer.nextToken()) {

			Part part = switch (token.getType()) {
				case MqlStringTemplateLexer.PLACEHOLDER -> new ParameterPart(index(token.getText().substring(1)));
				case MqlStringTemplateLexer.EXPRESSION -> new ExpressionPart(expression(token.getText()));
				default -> null;
			};

			if (part == null) {
				text.append(token.getText());
				continue;
			}

			if (!text.isEmpty()) {
				parts.add(new Text(text.toString()));
				text.setLength(0);
			}
			parts.add(part);
			bindings = true;
		}

		if (!bindings) {
			return null;
		}
		if (!text.isEmpty()) {
			parts.add(new Text(text.toString()));
		}

		boolean quoted = StringTemplate.isQuoted(content);
		return new StringTemplate(List.copyOf(parts), quoted, regex && quoted);
	}

	private TemplateNode placeholder(String token) {

		Matcher matcher = PLACEHOLDER_INDEX.matcher(token);
		return matcher.find() ? new Parameter(index(matcher.group(1))) : new Constant(token);
	}

	private Expression expression(String token) {

		if (token.charAt(1) == '$') {

			// ?${…} → ${…}, retained as is
			Expression expression = new Expression(token.substring(1), List.of());
			expressions.add(expression);
			return expression;
		}

		// ?#{…} → #{…}, placeholders rewritten to variables
		StringBuilder source = new StringBuilder(token.length()).append(token, 1, 3);
		List<Variable> variables = new ArrayList<>();

		Lexer lexer = new MqlExpressionLexer(CharStreams.fromString(token.substring(3, token.length() - 1)));
		lexer.removeErrorListeners();

		for (Token part = lexer.nextToken(); part.getType() != Token.EOF; part = lexer.nextToken()) {

			String text = part.getText();
			switch (part.getType()) {
				case MqlExpressionLexer.QUOTED_PLACEHOLDER ->
					source.append('#').append(variable(variables, index(text.substring(2, text.length() - 1)), true));
				case MqlExpressionLexer.PLACEHOLDER ->
					source.append('#').append(variable(variables, index(text.substring(1)), false));
				default -> source.append(text);
			}
		}

		Expression expression = new Expression(source.append('}').toString(), List.copyOf(variables));
		expressions.add(expression);
		return expression;
	}

	private static String variable(List<Variable> variables, int index, boolean quoted) {

		for (Variable variable : variables) {
			if (variable.index() == index && variable.quoted() == quoted) {
				return variable.name();
			}
		}

		Variable variable = new Variable("__QVar" + variables.size(), index, quoted);
		variables.add(variable);
		return variable.name();
	}

	private static TemplateNode name(String name) {

		return switch (name) {
			case "true" -> new Constant(Boolean.TRUE);
			case "false" -> new Constant(Boolean.FALSE);
			case "null" -> new Constant(null);
			case "undefined" -> new Literal(BsonType.UNDEFINED, new Constant(new BsonUndefined()));
			case "NaN" -> new Constant(Double.NaN);
			case "Infinity" -> new Constant(Double.POSITIVE_INFINITY);
			case "MinKey" -> new Constant(new MinKey());
			case "MaxKey" -> new Constant(new MaxKey());
			default -> {
				if (CONSTRUCTORS.contains(name)) {
					throw new JsonParseException("JSON reader expected '(' after '%s'.", name);
				}
				yield new Constant(name);
			}
		};
	}

	// -------------------------------------------------------------------------
	// Extended JSON
	// -------------------------------------------------------------------------

	/**
	 * @return {@literal null} if the entries do not form a wrapper and are to be read as a plain document.
	 */
	private @Nullable TemplateNode wrapper(String key, List<EntryContext> entries) {

		return switch (key) {
			case "$binary", "$type" -> binary(key, entries);
			case "$uuid" -> conversion(List.of(stringArgument(single(key, entries))), args -> {
				try {
					return new BsonBinary(BsonBinarySubType.UUID_STANDARD,
							decodeHex(requireString(args.get(0)).replace("-", "")));
				} catch (IllegalArgumentException e) {
					throw new JsonParseException(e);
				}
			});
			case "$regex", "$options" -> legacyRegularExpression(key, entries);
			case "$code" -> code(entries);
			case "$date" -> date(single(key, entries));
			case "$maxKey", "$minKey" -> {
				if (!(single(key, entries) instanceof NumberValueContext number
						&& Integer.valueOf(1).equals(number(number.NUMBER().getText())))) {
					throw new JsonParseException("JSON reader expected '1' as value of '%s'.", key);
				}
				yield new Constant("$maxKey".equals(key) ? new MaxKey() : new MinKey());
			}
			case "$oid" ->
				conversion(List.of(stringArgument(single(key, entries))), args -> new ObjectId(string(args.get(0))));
			case "$regularExpression" -> regularExpression(single(key, entries));
			case "$symbol" -> conversion(List.of(stringArgument(single(key, entries))), args -> requireString(args.get(0)));
			case "$timestamp" -> timestamp(single(key, entries));
			case "$undefined" -> {
				ValueContext value = single(key, entries);
				if (!"true".equals(value instanceof StringValueContext string ? unquote(string.STRING().getText())
						: value instanceof NameValueContext name ? name.getText() : null)) {
					throw new JsonParseException("JSON reader requires $undefined to have the value of true but found '%s'.",
							value.getText());
				}
				yield new Constant(new BsonUndefined());
			}
			case "$numberLong" -> conversion(List.of(stringArgument(single(key, entries))),
					args -> convertNumber(args.get(0), Long.class, Long::valueOf));
			case "$numberInt" -> conversion(List.of(stringArgument(single(key, entries))),
					args -> convertNumber(args.get(0), Integer.class, Integer::valueOf));
			case "$numberDouble" -> conversion(List.of(stringArgument(single(key, entries))),
					args -> convertNumber(args.get(0), Double.class, Double::valueOf));
			case "$numberDecimal" -> conversion(List.of(stringArgument(single(key, entries))),
					args -> convertNumber(args.get(0), Decimal128.class, Decimal128::parse));
			case "$dbPointer" -> dbPointer(single(key, entries));
			default -> null;
		};
	}

	private @Nullable TemplateNode binary(String key, List<EntryContext> entries) {

		if ("$binary".equals(key) && entries.get(0).value() instanceof ObjectValueContext object) {

			single(key, entries);
			List<EntryContext> nested = object.object().entry();
			String first = nested.isEmpty() ? null : literalKey(nested.get(0).key());
			int dataIndex = "base64".equals(first) ? 0 : "subType".equals(first) ? 1 : -1;

			if (dataIndex == -1 || nested.size() != 2
					|| !(dataIndex == 0 ? "subType" : "base64").equals(literalKey(nested.get(1).key()))) {
				throw new JsonParseException("Unexpected key for $binary: " + first);
			}

			byte type = binarySubType(nested.get(1 - dataIndex).value());
			return binary(stringArgument(nested.get(dataIndex).value()), type);
		}

		// legacy {$binary: …, $type: …} or {$type: …, $binary: …}, otherwise a plain document such as {$type: 2}
		if (entries.size() != 2) {
			return null;
		}

		boolean binaryFirst = "$binary".equals(key);
		if (!(binaryFirst ? "$type" : "$binary").equals(literalKey(entries.get(1).key()))) {
			return null;
		}

		ValueContext data = entries.get(binaryFirst ? 0 : 1).value();
		if (!isStringArgument(data)) {
			return null;
		}

		byte type;
		try {
			type = binarySubType(entries.get(binaryFirst ? 1 : 0).value());
		} catch (JsonParseException | NumberFormatException e) {
			return null;
		}

		return binary(stringArgument(data), type);
	}

	private static TemplateNode binary(TemplateNode data, byte type) {
		return conversion(List.of(data),
				args -> new BsonBinary(type, Base64.getDecoder().decode(requireString(args.get(0)))));
	}

	private static byte binarySubType(ValueContext value) {

		if (value instanceof StringValueContext string) {
			return (byte) Integer.parseInt(unquote(string.STRING().getText()), 16);
		}
		if (value instanceof NumberValueContext number && number(number.NUMBER().getText()) instanceof Integer subType) {
			return subType.byteValue();
		}
		throw new JsonParseException("JSON reader expected a string or number but found '%s'.", value.getText());
	}

	private @Nullable TemplateNode legacyRegularExpression(String key, List<EntryContext> entries) {

		boolean regexFirst = "$regex".equals(key);
		if (entries.size() != 2 || !(regexFirst ? "$options" : "$regex").equals(literalKey(entries.get(1).key()))
				|| !isStringArgument(entries.get(0).value()) || !isStringArgument(entries.get(1).value())) {
			return null;
		}

		return conversion(List.of(stringArgument(entries.get(0).value()), stringArgument(entries.get(1).value())),
				args -> new BsonRegularExpression(string(args.get(regexFirst ? 0 : 1)), string(args.get(regexFirst ? 1 : 0))));
	}

	private TemplateNode code(List<EntryContext> entries) {

		if (entries.size() == 1) {
			return conversion(List.of(stringArgument(entries.get(0).value())), args -> requireString(args.get(0)));
		}

		if (entries.size() == 2 && "$scope".equals(literalKey(entries.get(1).key()))) {

			TemplateNode code = stringArgument(entries.get(0).value());
			DocumentNode scope = document(entries(entries.get(1).value()));
			return conversion(List.of(code, scope),
					args -> new CodeWithScope(requireString(args.get(0)), (Document) args.get(1)));
		}

		throw new JsonParseException("JSON reader expected $code optionally followed by $scope but found %d entries.",
				entries.size());
	}

	private TemplateNode date(ValueContext value) {

		if (value instanceof ObjectValueContext object) {

			List<EntryContext> nested = object.object().entry();
			String key = nested.isEmpty() ? null : literalKey(nested.get(0).key());
			if (nested.size() != 1 || !"$numberLong".equals(key)) {
				throw new JsonParseException(format("JSON reader expected $numberLong within $date, but found %s", key));
			}

			return conversion(List.of(stringArgument(nested.get(0).value())),
					args -> new Date(convertNumber(args.get(0), Long.class, Long::valueOf)));
		}

		if (value instanceof NumberValueContext number) {

			if (!(number(number.NUMBER().getText()) instanceof Number millis) || millis instanceof Double) {
				throw new JsonParseException("JSON reader expected an integer or string but found '%s'.", value.getText());
			}
			long time = millis.longValue();
			return conversion(List.of(), args -> new Date(time));
		}

		if (isStringArgument(value)) {
			return conversion(List.of(stringArgument(value)), args -> new Date(dateTime(args.get(0))));
		}

		throw new JsonParseException("JSON reader expected an integer or string but found '%s'.", value.getText());
	}

	private static long dateTime(@Nullable Object value) {

		if (value instanceof Date date) {
			return date.getTime();
		}
		if (value instanceof Number number) {
			return NumberUtils.convertNumberToTargetClass(number, Long.class);
		}

		String dateTime = requireString(value);
		try {
			return DateTimeFormatter.parse(dateTime);
		} catch (DateTimeException e) {
			throw new JsonParseException(format("Failed to parse string '%s' as a date", dateTime), e);
		}
	}

	private TemplateNode regularExpression(ValueContext value) {

		List<EntryContext> nested = entries(value);
		String first = nested.size() == 2 ? literalKey(nested.get(0).key()) : null;
		boolean patternFirst = "pattern".equals(first);

		if (!patternFirst && !"options".equals(first)
				|| !(patternFirst ? "options" : "pattern").equals(literalKey(nested.get(1).key()))) {
			throw new JsonParseException("Expected 'pattern' and 'options' fields in $regularExpression document");
		}

		return conversion(List.of(stringArgument(nested.get(0).value()), stringArgument(nested.get(1).value())),
				args -> new BsonRegularExpression(string(args.get(patternFirst ? 0 : 1)),
						string(args.get(patternFirst ? 1 : 0))));
	}

	private static TemplateNode timestamp(ValueContext value) {

		List<EntryContext> nested = entries(value);
		String first = nested.size() == 2 ? literalKey(nested.get(0).key()) : null;
		boolean timeFirst = "t".equals(first);

		if (!timeFirst && !"i".equals(first) || !(timeFirst ? "i" : "t").equals(literalKey(nested.get(1).key()))) {
			throw new JsonParseException("Expected 't' and 'i' fields in $timestamp document");
		}

		int time = integer(nested.get(timeFirst ? 0 : 1).value(), true);
		int increment = integer(nested.get(timeFirst ? 1 : 0).value(), true);
		return new Constant(new BsonTimestamp(time, increment));
	}

	private TemplateNode dbPointer(ValueContext value) {

		List<EntryContext> nested = entries(value);
		String first = nested.size() == 2 ? literalKey(nested.get(0).key()) : null;
		boolean refFirst = "$ref".equals(first);

		if (!refFirst && !"$id".equals(first) || !(refFirst ? "$id" : "$ref").equals(literalKey(nested.get(1).key()))) {
			throw new JsonParseException("Expected $ref and $id fields in $dbPointer document but found " + first);
		}

		ValueContext id = nested.get(refFirst ? 1 : 0).value();
		List<EntryContext> oid = entries(id);
		if (oid.size() != 1 || !"$oid".equals(literalKey(oid.get(0).key()))) {
			throw new JsonParseException("JSON reader expected $oid within $id but found '%s'.", id.getText());
		}

		TemplateNode ref = stringArgument(nested.get(refFirst ? 0 : 1).value());
		TemplateNode objectId = stringArgument(oid.get(0).value());

		return conversion(refFirst ? List.of(ref, objectId) : List.of(objectId, ref),
				args -> new BsonDbPointer(requireString(args.get(refFirst ? 0 : 1)),
						new ObjectId(string(args.get(refFirst ? 1 : 0)))));
	}

	// -------------------------------------------------------------------------
	// Shell constructors
	// -------------------------------------------------------------------------

	private TemplateNode constructor(String name, @Nullable ArgumentsContext arguments, boolean newKeyword) {

		List<ValueContext> args = arguments != null ? arguments.value() : null;

		if ("MinKey".equals(name) || "MaxKey".equals(name)) {
			if (args != null && !args.isEmpty()) {
				throw new JsonParseException("JSON reader expected ')' after '%s('.", name);
			}
			return name(name);
		}

		if (args == null) {
			throw new JsonParseException("JSON reader expected '(' after '%s'.", name);
		}

		if (!newKeyword && "Date".equals(name)) {
			return conversion(List.of(), it -> new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH).format(new Date()));
		}

		return switch (name) {
			case "BinData" -> {
				arguments(name, args, 2, 2);
				byte type = (byte) integer(args.get(0), false);
				byte[] data = Base64.getDecoder().decode(rawString(args.get(1)));
				yield conversion(List.of(), it -> new BsonBinary(type, data.clone()));
			}
			case "UUID" -> {
				arguments(name, args, 1, 1);
				yield conversion(List.of(stringArgument(args.get(0))), it -> new BsonBinary(BsonBinarySubType.UUID_STANDARD,
						decodeHex(requireString(it.get(0)).replace("-", ""))));
			}
			case "RegExp" -> {
				arguments(name, args, 1, 2);
				List<TemplateNode> parts = new ArrayList<>();
				for (ValueContext arg : args) {
					parts.add(stringArgument(arg));
				}
				yield conversion(parts,
						it -> new BsonRegularExpression(string(it.get(0)), it.size() > 1 ? string(it.get(1)) : ""));
			}
			case "ObjectId" -> {
				arguments(name, args, 1, 1);
				yield conversion(List.of(stringArgument(args.get(0))), it -> new ObjectId(string(it.get(0))));
			}
			case "Timestamp" -> {
				arguments(name, args, 2, 2);
				yield new Constant(new BsonTimestamp(integer(args.get(0), false), integer(args.get(1), false)));
			}
			case "DBPointer" -> {
				arguments(name, args, 2, 2);
				yield conversion(List.of(stringArgument(args.get(0)), stringArgument(args.get(1))),
						it -> new BsonDbPointer(requireString(it.get(0)), new ObjectId(string(it.get(1)))));
			}
			case "NumberInt" -> {
				arguments(name, args, 1, 1);
				yield new Constant(
						args.get(0) instanceof StringValueContext string ? Integer.parseInt(unquote(string.STRING().getText()))
								: integer(args.get(0), false));
			}
			case "NumberLong" -> {
				arguments(name, args, 1, 1);
				yield new Constant(numberLong(args.get(0)));
			}
			case "NumberDecimal" -> {
				arguments(name, args, 1, 1);
				yield new Constant(numberDecimal(args.get(0)));
			}
			case "ISODate" -> {
				arguments(name, args, 0, 1);
				yield args.isEmpty() ? conversion(List.of(), it -> new Date()) : isoDate(args.get(0));
			}
			case "HexData" -> {
				arguments(name, args, 2, 2);
				int type = integer(args.get(0), false);
				yield conversion(List.of(stringArgument(args.get(1))), it -> hexData(type, requireString(it.get(0))));
			}
			case "Date" -> newDate(args);
			default -> throw new JsonParseException("JSON reader expected a type name but found '%s'.", name);
		};
	}

	private static long numberLong(ValueContext value) {

		if (value instanceof StringValueContext string) {
			return Long.parseLong(unquote(string.STRING().getText()));
		}
		return longInteger(value);
	}

	private static Decimal128 numberDecimal(ValueContext value) {

		if (value instanceof StringValueContext string) {
			return Decimal128.parse(unquote(string.STRING().getText()));
		}
		if (value instanceof NumberValueContext number) {

			Object n = number(number.NUMBER().getText());
			if (n instanceof Double d) {
				return new BsonDouble(d).decimal128Value();
			}
			return new Decimal128(((Number) n).longValue());
		}
		throw new JsonParseException("JSON reader expected a number or a string but found '%s'.", value.getText());
	}

	private static TemplateNode isoDate(ValueContext value) {

		if (!(value instanceof StringValueContext string)) {
			throw new JsonParseException("JSON reader expected a string but found '%s'.", value.getText());
		}

		String dateTime = unquote(string.STRING().getText());
		long time;
		try {
			time = DateTimeFormatter.parse(dateTime);
		} catch (DateTimeException e) {
			throw new JsonParseException("Failed to parse string as a date: " + dateTime, e);
		}
		return conversion(List.of(), it -> new Date(time));
	}

	private static BsonBinary hexData(int type, String hex) {

		String value = (hex.length() & 1) != 0 ? "0" + hex : hex;
		for (BsonBinarySubType subType : BsonBinarySubType.values()) {
			if (subType.getValue() == type) {
				return new BsonBinary(subType, decodeHex(value));
			}
		}
		return new BsonBinary(decodeHex(value));
	}

	private static TemplateNode newDate(List<ValueContext> args) {

		if (args.isEmpty()) {
			return conversion(List.of(), it -> new Date());
		}

		if (args.size() == 1 && args.get(0) instanceof StringValueContext string) {

			String text = unquote(string.STRING().getText());
			ParsePosition position = new ParsePosition(0);
			Date date = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH).parse(text, position);
			if (date == null || position.getIndex() != text.length()) {
				throw new JsonParseException(
						"JSON reader expected a date in 'EEE MMM dd yyyy HH:mm:ss z' format but found '%s'.", text);
			}
			long time = date.getTime();
			return conversion(List.of(), it -> new Date(time));
		}

		if (args.size() != 1 && (args.size() < 3 || args.size() > 7)) {
			throw new JsonParseException("JSON reader expected 1 or 3-7 integers but found %d.", args.size());
		}

		long[] values = new long[7];
		for (int i = 0; i < args.size(); i++) {
			values[i] = longInteger(args.get(i));
		}

		if (args.size() == 1) {
			long time = values[0];
			return conversion(List.of(), it -> new Date(time));
		}

		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.set(Calendar.YEAR, (int) values[0]);
		calendar.set(Calendar.MONTH, (int) values[1]);
		calendar.set(Calendar.DAY_OF_MONTH, (int) values[2]);
		calendar.set(Calendar.HOUR_OF_DAY, (int) values[3]);
		calendar.set(Calendar.MINUTE, (int) values[4]);
		calendar.set(Calendar.SECOND, (int) values[5]);
		calendar.set(Calendar.MILLISECOND, (int) values[6]);
		long time = calendar.getTimeInMillis();
		return conversion(List.of(), it -> new Date(time));
	}

	private static long longInteger(ValueContext value) {

		if (value instanceof NumberValueContext number && number(number.NUMBER().getText()) instanceof Number n
				&& !(n instanceof Double)) {
			return n.longValue();
		}
		throw new JsonParseException("JSON reader expected an integer but found '%s'.", value.getText());
	}

	private static void arguments(String name, List<ValueContext> args, int min, int max) {

		if (args.size() < min || args.size() > max) {
			throw new JsonParseException("JSON reader found an invalid number of arguments (%d) for '%s'.", args.size(),
					name);
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	/**
	 * Creates a {@link Conversion} that is applied once if all arguments are constant to fail early on invalid input.
	 */
	private static TemplateNode conversion(List<TemplateNode> arguments, Conversion.Converter converter) {

		List<@Nullable Object> constants = new ArrayList<>(arguments.size());
		for (TemplateNode argument : arguments) {
			if (!(argument instanceof Constant constant)) {
				return new Conversion(arguments, converter);
			}
			constants.add(constant.value());
		}

		converter.convert(constants);
		return new Conversion(arguments, converter);
	}

	private static boolean isStringArgument(ValueContext value) {
		return value instanceof StringValueContext || value instanceof NameValueContext
				|| value instanceof PlaceholderValueContext || value instanceof ExpressionValueContext;
	}

	/**
	 * Quoted or unquoted string, placeholder or expression; read via {@link #string(Object)}.
	 */
	private TemplateNode stringArgument(ValueContext value) {

		if (value instanceof NameValueContext name) {
			return new Constant(name.getText());
		}
		if (isStringArgument(value)) {
			return visit(value);
		}
		throw new JsonParseException("JSON reader expected a string but found '%s'.", value.getText());
	}

	/**
	 * Unbound string content as read by legacy constructors.
	 */
	private static String rawString(ValueContext value) {

		if (value instanceof StringValueContext string) {
			return unquote(string.STRING().getText());
		}
		if (isStringArgument(value)) {
			return value.getText();
		}
		throw new JsonParseException("JSON reader expected a string but found '%s'.", value.getText());
	}

	private static int integer(ValueContext value, boolean allowLong) {

		if (value instanceof NumberValueContext number) {

			Object n = number(number.NUMBER().getText());
			if (n instanceof Integer i) {
				return i;
			}
			if (allowLong && n instanceof Long l) {
				return l.intValue();
			}
		}
		throw new JsonParseException("JSON reader expected an integer but found '%s'.", value.getText());
	}

	private static ValueContext single(String key, List<EntryContext> entries) {

		if (entries.size() != 1) {
			throw new JsonParseException("JSON reader expected a single entry within '%s' document but found %d.", key,
					entries.size());
		}
		return entries.get(0).value();
	}

	private static List<EntryContext> entries(ValueContext value) {

		if (value instanceof ObjectValueContext object) {
			return object.object().entry();
		}
		throw new JsonParseException("JSON reader expected a document but found '%s'.", value.getText());
	}

	private static @Nullable String literalKey(KeyContext key) {

		if (key.STRING() != null) {
			return unquote(key.STRING().getText());
		}
		return key.NAME() != null || key.NEW() != null ? key.getText() : null;
	}

	private static @Nullable String string(@Nullable Object value) {
		return value != null ? value.toString() : null;
	}

	private static String requireString(@Nullable Object value) {

		if (value == null) {
			throw new IllegalArgumentException("Value must not be null");
		}
		return value.toString();
	}

	private static <T> T convertNumber(@Nullable Object value, Class<T> type,
			java.util.function.Function<String, T> parser) {

		String source = requireString(value);
		try {
			return parser.apply(source);
		} catch (NumberFormatException e) {
			throw new JsonParseException(format("Exception converting value '%s' to type %s", source, type.getName()), e);
		}
	}

	private static Object number(String text) {

		if ("-Infinity".equals(text)) {
			return Double.NEGATIVE_INFINITY;
		}

		try {
			if (text.indexOf('.') != -1 || text.indexOf('e') != -1 || text.indexOf('E') != -1) {
				return Double.parseDouble(text);
			}

			long value = Long.parseLong(text);
			if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
				return value;
			}
			return (int) value;
		} catch (NumberFormatException e) {
			throw new JsonParseException(format("Invalid JSON number '%s'", text), e);
		}
	}

	private static int index(String digits) {

		try {
			return Integer.parseInt(digits);
		} catch (NumberFormatException e) {
			throw new JsonParseException(format("Invalid parameter index '%s'", digits), e);
		}
	}

	static String unquote(String token) {

		String content = token.substring(1, token.length() - 1);
		if (content.indexOf('\\') == -1) {
			return content;
		}

		StringBuilder result = new StringBuilder(content.length());
		for (int i = 0; i < content.length(); i++) {

			char c = content.charAt(i);
			if (c != '\\') {
				result.append(c);
				continue;
			}

			char escaped = content.charAt(++i);
			switch (escaped) {
				case '\'', '"', '\\', '/' -> result.append(escaped);
				case 'b' -> result.append('\b');
				case 'f' -> result.append('\f');
				case 'n' -> result.append('\n');
				case 'r' -> result.append('\r');
				case 't' -> result.append('\t');
				case 'u' -> {
					if (i + 4 >= content.length()) {
						throw new JsonParseException("Invalid unicode escape sequence in JSON string");
					}
					try {
						result.append((char) Integer.parseInt(content.substring(i + 1, i + 5), 16));
					} catch (NumberFormatException e) {
						throw new JsonParseException("Invalid unicode escape sequence in JSON string", e);
					}
					i += 4;
				}
				default -> result.append('\\').append(escaped);
			}
		}
		return result.toString();
	}

	private static byte[] decodeHex(String hex) {

		if (hex.length() % 2 != 0) {
			throw new IllegalArgumentException("A hex string must contain an even number of characters: " + hex);
		}

		byte[] out = new byte[hex.length() / 2];
		for (int i = 0; i < hex.length(); i += 2) {

			int high = Character.digit(hex.charAt(i), 16);
			int low = Character.digit(hex.charAt(i + 1), 16);
			if (high == -1 || low == -1) {
				throw new IllegalArgumentException("A hex string can only contain the characters 0-9, A-F, a-f: " + hex);
			}
			out[i / 2] = (byte) (high * 16 + low);
		}
		return out;
	}

	private static class ThrowingErrorListener extends BaseErrorListener {

		static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, @Nullable Object offendingSymbol, int line,
				int charPositionInLine, String msg, @Nullable RecognitionException e) {
			throw new JsonParseException("Invalid JSON at line %d:%d; %s", line, charPositionInLine, msg);
		}
	}

	private static class NestingDepthLimit implements ParseTreeListener {

		private int depth;

		@Override
		public void enterEveryRule(ParserRuleContext ctx) {

			if (isNesting(ctx) && ++depth > MAX_NESTING_DEPTH) {
				throw new JsonParseException("JSON template exceeds maximum nesting depth of %d", MAX_NESTING_DEPTH);
			}
		}

		@Override
		public void exitEveryRule(ParserRuleContext ctx) {

			if (isNesting(ctx)) {
				depth--;
			}
		}

		@Override
		public void visitTerminal(TerminalNode node) {}

		@Override
		public void visitErrorNode(ErrorNode node) {}

		private static boolean isNesting(ParserRuleContext ctx) {
			int rule = ctx.getRuleIndex();
			return rule == MqlTemplateParser.RULE_object || rule == MqlTemplateParser.RULE_array
					|| rule == MqlTemplateParser.RULE_arguments;
		}
	}
}
