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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonParseException;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ParseException;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Unit tests for {@link MqlTemplate} via {@link ParameterBindingDocumentCodec}.
 *
 * @author Christoph Strobl
 */
class MqlTemplateUnitTests {

	@Test
	void bindsValuesRetainingTheirType() {

		assertThat(parse("{ 'lastname' : ?0, 'age' : ?1, 'tags' : ?2, 'filter' : ?3 }", "kohlin", 100,
				List.of("a", "b"), new Document("$eq", 1)))
				.isEqualTo(new Document("lastname", "kohlin").append("age", 100).append("tags", List.of("a", "b"))
						.append("filter", new Document("$eq", 1)));
	}

	@Test
	void bindsQuotedValuesAsString() {

		assertThat(parse("{ 'lastname' : '?0', 'nil' : '?1' }", 100, null))
				.isEqualTo(new Document("lastname", "100").append("nil", "null"));
	}

	@Test
	void bindsKeys() {
		assertThat(parse("{ ?0 : ?1, '?0_suffix' : 1 }", "firstname", "kaladin"))
				.isEqualTo(new Document("firstname", "kaladin").append("firstname_suffix", 1));
	}

	@Test
	void doesNotReplaceOverlappingParameterIndexes() {

		Object[] args = { "zero", "ONE", 2, 3, 4, 5, 6, 7, 8, 9, "TEN" };

		assertThat(parse("{ a : '?1|?10' }", args)).isEqualTo(new Document("a", "ONE|TEN"));
		assertThat(parse("{ a : /?1|?10/ }", args).get("a")).isEqualTo(new BsonRegularExpression("ONE|TEN"));
		assertThat(parse("{ a : ?#{ ?1 + ':' + ?10 } }", args)).isEqualTo(new Document("a", "ONE:TEN"));
	}

	@Test
	void doesNotInterpretBoundValuesAsTemplate() {

		assertThat(parse("{ a : '?0|?1' }", "?1", "SECOND")).isEqualTo(new Document("a", "?1|SECOND"));
		assertThat(parse("{ a : '?0' }", "?#{1 + 1}")).isEqualTo(new Document("a", "?#{1 + 1}"));
		assertThat(parse("{ a : ?0 }", "{ '$ne' : null }")).isEqualTo(new Document("a", "{ '$ne' : null }"));
		assertThat(parse("{ a : :#{?0} }", "' + T(java.lang.Runtime) + '"))
				.isEqualTo(new Document("a", "' + T(java.lang.Runtime) + '"));
	}

	@Test
	void parsesBoundStringAtRootAsPlainJson() {

		assertThat(parse("?0", "{ 'itWorks' : true }")).isEqualTo(new Document("itWorks", true));
		assertThatIllegalArgumentException().isThrownBy(() -> parse("?0", "{ 'a' : ?1 }"));
		assertThatIllegalArgumentException().isThrownBy(() -> parse("?0", ":#{new org.bson.Document('foo', 'bar')}"));
	}

	@Test
	void bindsDocumentAndMapAtRoot() {

		Document source = new Document("a", 1);

		assertThat(parse("?0", source)).isSameAs(source);
		assertThat(parse(" :#{?0} ", source)).isSameAs(source);
		assertThat(parse("?0", Map.of("a", 1))).isEqualTo(source);
		assertThat(parse("?0", (Object) null)).isEmpty();
	}

	@Test
	void rejectsNonDocumentRootBinding() {
		assertThatIllegalArgumentException().isThrownBy(() -> parse("?0", 5));
	}

	@ParameterizedTest
	@ValueSource(strings = { "[1, 2]", "'abc'", "{ a : }", "{ , }", "{ a : [1,,2] }", "{ a : 'x }", "{ a : 1", "{ a : 1b }",
			"{ a : /abc/g }", "{ a : # }", "{ a : new }", "{ a : new Foo() }", "{ a : foo(1) }", "{ a : ObjectId }",
			"{ a : { $oid : '5f3e8b8b8b8b8b8b8b8b8b8b', b : 1 } }", "{ a : ?#{1 }" })
	void rejectsInvalidSyntax(String json) {
		assertThatExceptionOfType(JsonParseException.class).isThrownBy(() -> parse(json));
	}

	@Test
	void retainsLenientSyntax() {

		assertThat(parse("{ a : 1 b : 2 }")).isEqualTo(new Document("a", 1).append("b", 2));
		assertThat(parse("{ a : [1 2], }")).isEqualTo(new Document("a", List.of(1, 2)));
		assertThat(parse("{ a : 1 }} trailing")).isEqualTo(new Document("a", 1));
		assertThat(parse("{ a : ?0suffix }", 7)).isEqualTo(new Document("a", 7));
	}

	@Test
	void rejectsExcessiveNesting() {

		String json = "{ a : " + "[".repeat(MqlTemplateCompiler.MAX_NESTING_DEPTH) + "]".repeat(
				MqlTemplateCompiler.MAX_NESTING_DEPTH) + " }";

		assertThatExceptionOfType(JsonParseException.class).isThrownBy(() -> parse(json))
				.withMessageContaining("nesting depth");
	}

	@Test
	void rejectsExcessiveConstructorNesting() {

		String json = "{ a : " + "NumberLong(".repeat(20_000) + "1" + ")".repeat(20_000) + " }";

		assertThatExceptionOfType(JsonParseException.class).isThrownBy(() -> parse(json))
				.withMessageContaining("nesting depth");
	}

	@Test
	void evaluatesExpressions() {

		assertThat(parse("{ a : ?#{ [0] }, b : :#{ ?0 }, c : ?#{ '?1' } }", 1.5D, 10))
				.isEqualTo(new Document("a", 1.5D).append("b", 1.5D).append("c", "10"));
		assertThat(parse("{ 'id' : ?#{ [0] ? { $exists : true } : [1] } }", true, "x"))
				.isEqualTo(new Document("id", new Document("$exists", true)));
	}

	@Test
	void evaluatesExpressionContainingBracesInStrings() {
		assertThat(parse("{ a : ?#{ '}' + '{' } }")).isEqualTo(new Document("a", "}{"));
	}

	@Test
	void evaluatesMultipleExpressionsAndParametersInString() {
		assertThat(parse("{ a : '?#{1}-?#{2} ?0' }", "x")).isEqualTo(new Document("a", "1-2 x"));
	}

	@Test
	void retainsRegexOptionsForExpressions() {
		assertThat(parse("{ a : /^?#{'foo'}$/i }").get("a")).isEqualTo(new BsonRegularExpression("^foo$", "i"));
	}

	@Test
	void readsRegexContainingSlashWithinExpression() {

		assertThat(parse("{ a : /^?#{6 / 2}$/ }").get("a")).isEqualTo(new BsonRegularExpression("^3$"));
		assertThat(parse("{ a : /^?#{'a/b'}$/i }").get("a")).isEqualTo(new BsonRegularExpression("^a/b$", "i"));
	}

	@Test
	void retainsDoubledQuotesWithinExpressionStrings() {

		assertThat(parse("{ a : ?#{'a''?0'} }", "x").get("a")).isEqualTo("a'?0");
		assertThat(parse("{ a : ?#{\"a\"\"?0\"} }", "x").get("a")).isEqualTo("a\"?0");
		assertThat(parse("{ a : ?#{'?0' + 'b'} }", "x").get("a")).isEqualTo("xb");
	}

	@Test
	void resolvesPropertyPlaceholdersIndependentOfExpressionQuoting() {

		ParameterBindingContext context = new ParameterBindingContext(index -> "x", propertyEvaluator());

		assertThat(codec().decode("{ a : ?${missing:O'Reilly} }", context)).isEqualTo(new Document("a", "O'Reilly"));
		assertThat(codec().decode("{ a : ?${missing:say \"hi} }", context)).isEqualTo(new Document("a", "say \"hi"));
		assertThat(codec().decode("{ a : \"x ?${missing:O'Reilly} y\" }", context))
				.isEqualTo(new Document("a", "x O'Reilly y"));
		assertThat(codec().decode("{ a : ?${missing:{b}} }", context)).isEqualTo(new Document("a", "{b}"));
		assertThat(codec().decode("{ a : ?${missing:?0} }", context)).isEqualTo(new Document("a", "?0"));
	}

	@Test
	void escapesQuotedRegexBindings() {

		assertThat(parse("{ 'value': /^\\Q?0\\E/ }", "\\E.*").get("value"))
				.isEqualTo(new BsonRegularExpression("^\\Q\\E\\\\E\\Q.*\\E"));
		assertThat(parse("{ 'value': /^\\Q:#{?0}\\E/ }", "\\E.*").get("value"))
				.isEqualTo(new BsonRegularExpression("^\\Q\\E\\\\E\\Q.*\\E"));
		assertThat(parse("{ 'value': '^\\Q:#{?0}\\E' }", "\\E.").get("value")).isEqualTo("^\\Q\\E.\\E");
	}

	@Test
	void evaluatesEachExpressionOnce() {

		AtomicInteger counter = new AtomicInteger();
		Document target = parse("{ a : { $regex : ?#{1}, $options : 'i' }, b : [ ?#{2} ], c : '?#{3}' }",
				countingEvaluator(counter));

		assertThat(target.get("a")).isEqualTo(new BsonRegularExpression("v", "i"));
		assertThat(counter).hasValue(3);
	}

	@Test
	void doesNotEvaluateExpressionsOfInvalidTemplate() {

		AtomicInteger counter = new AtomicInteger();

		assertThatIllegalArgumentException()
				.isThrownBy(() -> parse("{ a : ?#{1}, b : ObjectId('invalid') }", countingEvaluator(counter)));
		assertThatExceptionOfType(JsonParseException.class)
				.isThrownBy(() -> parse("{ a : ?#{1}, b : }", countingEvaluator(counter)));
		assertThat(counter).hasValue(0);
	}

	@Test
	void evaluatesEntireQueryExpression() {

		Object[] args = { "expected", "unexpected" };

		assertThat(parse("?#{ true ? { 'name': ?0 } : { 'name' : ?1 } }", args)).isEqualTo(new Document("name", "expected"));
		assertThat(parse("?#{ true ? { 'name': ?0 } : { 'name' : ?1 } }", UUID.randomUUID(), "x").get("name"))
				.isInstanceOf(UUID.class);
		assertThatExceptionOfType(ParseException.class)
				.isThrownBy(() -> parse("?#{ true ? { 'name': ?0 { } } : { 'name' : ?1 } }", args));
	}

	@Test
	void evaluatesExpressionsAgainstEvaluationContext() {

		Object[] args = { "yess", "nooo" };
		StandardEvaluationContext evaluationContext = (StandardEvaluationContext) EvaluationContextProvider.DEFAULT
				.getEvaluationContext(args);
		evaluationContext.setRootObject(new DummySecurityObject(new DummyWithId("wonderwoman")));
		ParameterBindingContext context = new ParameterBindingContext((index) -> args[index], new SpelExpressionParser(),
				() -> evaluationContext);

		String type = getClass().getName();

		assertThat(codec().decode("{ 'isBatman' : ?#{ T(" + type + ").isBatman() ? '?0' : '?1' }}", context))
				.isEqualTo(new Document("isBatman", "nooo"));
		assertThat(codec().decode("{ 'isBatman' : \"?#{ T(" + type + ").isBatman() ? '?0' : '?1' }\" }", context))
				.isEqualTo(new Document("isBatman", "nooo"));
		assertThat(codec().decode("?#{ T(" + type + ").applyFilterByUser('?0', principal.id) }", context))
				.isEqualTo(new Document("$and", Arrays.asList(new Document("_class", new Document("$eq", "yess")),
						new Document("user.supervisor", "wonderwoman"))));
	}

	@Test
	void doesNotAccessEvaluationContextWithoutExpressions() {

		EvaluationContext evaluationContext = new StandardEvaluationContext() {

			@Override
			public TypedValue getRootObject() {
				throw new IllegalStateException("o_O");
			}
		};

		assertThat(codec().decode("{ 'name' : '?0' }",
				new ParameterBindingContext((index) -> "value", new SpelExpressionParser(), () -> evaluationContext)))
				.isEqualTo(new Document("name", "value"));
	}

	@Test
	void capturesExpressionDependenciesWithoutEvaluation() {

		ValueExpressionParser parser = ValueExpressionParser.create(SpelExpressionParser::new);

		assertThat(codec().captureExpressionDependencies("{ a : ?0 }", index -> 1, parser))
				.isEqualTo(ExpressionDependencies.none());
		assertThat(codec().captureExpressionDependencies(
				"{ $and : [?#{ [0] == null ? { '$where' : 'true' } : { 'v1' : { '$in' : {[0]} } } }]}", index -> 1, parser))
				.isEqualTo(ExpressionDependencies.none());
		assertThat(codec().captureExpressionDependencies("{ hello : ?#{hasRole('foo')} }", index -> {
			throw new IllegalStateException();
		}, parser).get()).hasSize(1);
	}

	@Test
	void parsesExtendedJson() {

		Document target = parse("""
				{
				  oid : { $oid : '5f3e8b8b8b8b8b8b8b8b8b8b' },
				  date : { $date : '2015-04-16T16:55:57.626+02:00' },
				  dateLong : { $date : { $numberLong : '1' } },
				  long : { $numberLong : '12' },
				  int : { $numberInt : '3' },
				  double : { $numberDouble : '1.5' },
				  decimal : { $numberDecimal : '1.1' },
				  binary : { $binary : { base64 : 'AQI=', subType : '00' } },
				  legacyBinary : { $type : '00', $binary : 'AQI=' },
				  uuid : { $uuid : 'cfbca728-4e39-4613-96bc-f920b5c37e16' },
				  regex : { $regularExpression : { pattern : 'a', options : 'i' } },
				  legacyRegex : { $options : 'i', $regex : 'a' },
				  timestamp : { $timestamp : { t : 1, i : 2 } },
				  minKey : { $minKey : 1 },
				  maxKey : { $maxKey : 1 },
				  undefined : { $undefined : true },
				  symbol : { $symbol : 's' },
				  code : { $code : 'x()' },
				  pointer : { $dbPointer : { $id : { $oid : '5f3e8b8b8b8b8b8b8b8b8b8b' }, $ref : 'c' } },
				  type : { $type : 2 },
				  ref : { $ref : 'c', $id : ?0 }
				}
				""", "id");

		assertThat(target.get("oid")).isEqualTo(new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b"));
		assertThat(target.get("date")).isEqualTo(new Date(1429196157626L));
		assertThat(target.get("dateLong")).isEqualTo(new Date(1));
		assertThat(target.get("long")).isEqualTo(12L);
		assertThat(target.get("int")).isEqualTo(3);
		assertThat(target.get("double")).isEqualTo(1.5D);
		assertThat(target.get("decimal")).isEqualTo(Decimal128.parse("1.1"));
		assertThat(target.get("binary")).isEqualTo(new BsonBinary(new byte[] { 1, 2 }));
		assertThat(target.get("legacyBinary")).isEqualTo(new BsonBinary(new byte[] { 1, 2 }));
		assertThat(target.get("uuid", BsonBinary.class).getType()).isEqualTo(BsonBinarySubType.UUID_STANDARD.getValue());
		assertThat(target.get("regex")).isEqualTo(new BsonRegularExpression("a", "i"));
		assertThat(target.get("legacyRegex")).isEqualTo(new BsonRegularExpression("a", "i"));
		assertThat(target.get("timestamp")).isEqualTo(new BsonTimestamp(1, 2));
		assertThat(target.get("minKey")).isInstanceOf(MinKey.class);
		assertThat(target.get("maxKey")).isInstanceOf(MaxKey.class);
		assertThat(target.get("undefined")).isInstanceOf(BsonUndefined.class);
		assertThat(target.get("symbol")).isEqualTo("s");
		assertThat(target.get("code")).isEqualTo("x()");
		assertThat(target.get("pointer"))
				.isEqualTo(new BsonDbPointer("c", new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b")));
		assertThat(target.get("type")).isEqualTo(new Document("$type", 2));
		assertThat(target.get("ref")).isEqualTo(new Document("$ref", "c").append("$id", "id"));
	}

	@Test
	void bindsExtendedJsonValues() {

		Date date = new Date();

		assertThat(parse("{ a : { $date : ?0 }, b : { $date : '?0' } }", date))
				.isEqualTo(new Document("a", date).append("b", date));
		assertThat(parse("{ a : { $date : ?0 } }", date.getTime())).isEqualTo(new Document("a", date));
		assertThat(parse("{ a : { $date : ?0 } }", "2019-07-04T12:19:23.000Z"))
				.isEqualTo(new Document("a", new Date(1562242763000L)));
		assertThat(parse("{ a : { $oid : ?0 }, b : ObjectId(?0) }", "5f3e8b8b8b8b8b8b8b8b8b8b"))
				.isEqualTo(new Document("a", new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b")).append("b",
						new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b")));
		assertThat(parse("{ a : { $regex : '.*?0.*', $options : 'i' } }", "calamity"))
				.isEqualTo(new Document("a", new BsonRegularExpression(".*calamity.*", "i")));
		assertThatIllegalArgumentException().isThrownBy(() -> parse("{ 'value' : ObjectId(?0) }", (Object) null))
				.withMessageContaining("hexString");
	}

	@Test
	void parsesShellConstructors() {

		Document target = parse("""
				{
				  oid : ObjectId('5f3e8b8b8b8b8b8b8b8b8b8b'),
				  isoDate : ISODate('1970-01-01T00:00:00Z'),
				  date : new Date(0),
				  dateParts : new Date(2020, 1, 2),
				  int : NumberInt('2'),
				  long : NumberLong(3),
				  decimal : NumberDecimal(1.5),
				  binary : BinData(0, 'AQI='),
				  hex : HexData(0, '102'),
				  uuid : UUID('cfbca728-4e39-4613-96bc-f920b5c37e16'),
				  regex : RegExp('a', 'i'),
				  timestamp : Timestamp(1, 2),
				  pointer : DBPointer('ns', '5f3e8b8b8b8b8b8b8b8b8b8b'),
				  min : MinKey,
				  max : new MaxKey()
				}
				""");

		assertThat(target.get("oid")).isEqualTo(new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b"));
		assertThat(target.get("isoDate")).isEqualTo(new Date(0));
		assertThat(target.get("date")).isEqualTo(new Date(0));
		assertThat(target.get("dateParts")).isEqualTo(new Date(1580601600000L));
		assertThat(target.get("int")).isEqualTo(2);
		assertThat(target.get("long")).isEqualTo(3L);
		assertThat(target.get("decimal")).isEqualTo(Decimal128.parse("1.5"));
		assertThat(target.get("binary")).isEqualTo(new BsonBinary(new byte[] { 1, 2 }));
		assertThat(target.get("hex")).isEqualTo(new BsonBinary(new byte[] { 1, 2 }));
		assertThat(target.get("uuid", BsonBinary.class).getType()).isEqualTo(BsonBinarySubType.UUID_STANDARD.getValue());
		assertThat(target.get("regex")).isEqualTo(new BsonRegularExpression("a", "i"));
		assertThat(target.get("timestamp")).isEqualTo(new BsonTimestamp(1, 2));
		assertThat(target.get("pointer")).isEqualTo(new BsonDbPointer("ns", new ObjectId("5f3e8b8b8b8b8b8b8b8b8b8b")));
		assertThat(target.get("min")).isInstanceOf(MinKey.class);
		assertThat(target.get("max")).isInstanceOf(MaxKey.class);
	}

	@Test
	void appliesTransformerToNestedDocuments() {

		List<Object> transformed = new ArrayList<>();
		ParameterBindingDocumentCodec codec = new ParameterBindingDocumentCodec(registry(), new BsonTypeClassMap(),
				value -> {
					transformed.add(value);
					return value;
				}, true);

		codec.decode("{ a : 1, b : 'x', c : { d : 2 }, e : [ 3, { f : 4 } ] }");

		assertThat(transformed).containsExactly(new Document("d", 2), new Document("f", 4));
	}

	@ParameterizedTest
	@ValueSource(strings = { "{ a : { b : { c : 1 } } }", "{ d : [ { e : 2 } ] }" })
	void decodesNestedDocumentsWithRegisteredDocumentCodec(String json) {

		CodecRegistry registry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new MarkingDocumentCodec()),
				registry());

		Document antlr = new ParameterBindingDocumentCodec(registry, new BsonTypeClassMap(), null, true).decode(json);
		Document legacy = new ParameterBindingDocumentCodec(registry, new BsonTypeClassMap(), null, false).decode(json);

		assertThat(antlr).isEqualTo(legacy);
	}

	@Test
	void decodesSubtreeOnceWithRegisteredDocumentCodec() {

		CodecRegistry registry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new MarkingDocumentCodec()),
				registry());

		Document target = new ParameterBindingDocumentCodec(registry, new BsonTypeClassMap(), null, true)
				.decode("{ a : { b : { c : 1 } }, d : [ { e : 2 } ] }");

		assertThat(target).isEqualTo(new Document("a", new Document("b", new Document("c", 1)).append("marker", true))
				.append("d", List.of(new Document("e", 2).append("marker", true))));
	}

	@Test
	void bindsParametersBeforeDecodingWithRegisteredDocumentCodec() {

		CodecRegistry registry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new MarkingDocumentCodec()),
				registry());

		Document target = new ParameterBindingDocumentCodec(registry, new BsonTypeClassMap(), null, true)
				.decode("{ a : { b : ?0 } }", "value");

		assertThat(target).isEqualTo(new Document("a", new Document("b", "value").append("marker", true)));
	}

	@Test
	void decodesNestedDocumentsUsingBsonTypeClassMap() {

		CodecRegistry registry = CodecRegistries.fromRegistries(registry(),
				CodecRegistries.fromProviders(new BsonValueCodecProvider()));
		BsonTypeClassMap classMap = new BsonTypeClassMap(Map.of(BsonType.DOCUMENT, BsonDocument.class));
		String json = "{ a : { b : 1 } }";

		Document antlr = new ParameterBindingDocumentCodec(registry, classMap, null, true).decode(json);
		Document legacy = new ParameterBindingDocumentCodec(registry, classMap, null, false).decode(json);

		assertThat(antlr).isEqualTo(legacy).isEqualTo(new Document("a", new BsonDocument("b", new BsonInt32(1))));
	}

	@Test
	void readsCodeWithScope() {

		assertThat(parse("{ a : { $code : 'x', $scope : { v : ?0 } }, b : 2 }", "p"))
				.isEqualTo(new Document("a", new CodeWithScope("x", new Document("v", "p"))).append("b", 2));
	}

	@Test
	void readsCode() {
		assertThat(parse("{ a : { $code : ?0 } }", "x")).isEqualTo(new Document("a", "x"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "{ a : { $code : 'x', $scope : 1 } }", "{ a : { $code : 'x', $scope : { v : 1 }, c : 3 } }",
			"{ a : { $code : 'x', c : 3 } }" })
	void rejectsInvalidCodeWithScope(String json) {
		assertThatExceptionOfType(JsonParseException.class).isThrownBy(() -> parse(json));
	}

	@Test
	void createsNewValuesForEachBinding() {

		String json = "{ a : { b : [ 1 ] }, c : BinData(0, 'AQI='), d : ISODate('1970-01-01T00:00:00Z') }";

		Document first = parse(json);
		first.get("a", Document.class).put("b", "changed");
		first.get("c", BsonBinary.class).getData()[0] = 9;
		first.get("d", Date.class).setTime(1);

		assertThat(parse(json)).isEqualTo(new Document("a", new Document("b", List.of(1)))
				.append("c", new BsonBinary(new byte[] { 1, 2 })).append("d", new Date(0)));
	}

	private static Document parse(String json, Object... args) {
		return codec().decode(json, args);
	}

	private static Document parse(String json, ValueExpressionEvaluator evaluator) {
		return codec().decode(json, new ParameterBindingContext(index -> null, evaluator));
	}

	private static ValueExpressionEvaluator countingEvaluator(AtomicInteger counter) {

		return new ValueExpressionEvaluator() {

			@Override
			@SuppressWarnings("unchecked")
			public <T> T evaluate(String expression) {
				counter.incrementAndGet();
				return (T) "v";
			}
		};
	}

	private static ValueExpressionEvaluator propertyEvaluator() {

		ValueEvaluationContext context = ValueEvaluationContext.of(new StandardEnvironment(),
				new StandardEvaluationContext());

		return new ValueExpressionEvaluator() {

			@Override
			@SuppressWarnings("unchecked")
			public <T> T evaluate(String expression) {
				return (T) ValueExpressionParser.create().parse(expression).evaluate(context);
			}
		};
	}

	private static ParameterBindingDocumentCodec codec() {
		return new ParameterBindingDocumentCodec(registry(), new BsonTypeClassMap(), null, true);
	}

	private static org.bson.codecs.configuration.CodecRegistry registry() {
		return CodecRegistries.fromProviders(new ValueCodecProvider(), new BsonValueCodecProvider(),
				new DocumentCodecProvider());
	}

	public static boolean isBatman() {
		return false;
	}

	public static String applyFilterByUser(String _class, String username) {
		return "{ '$and' : [ {'_class': { '$eq' : '" + _class + "' } }, {'user.supervisor':  '" + username + "' } ] }";
	}

	static class MarkingDocumentCodec implements Codec<Document> {

		private final DocumentCodec delegate = new DocumentCodec();

		@Override
		public Document decode(BsonReader reader, DecoderContext decoderContext) {
			return delegate.decode(reader, decoderContext).append("marker", true);
		}

		@Override
		public void encode(BsonWriter writer, Document value, EncoderContext encoderContext) {
			delegate.encode(writer, value, encoderContext);
		}

		@Override
		public Class<Document> getEncoderClass() {
			return Document.class;
		}
	}

	public record DummySecurityObject(DummyWithId principal) {

		public DummyWithId getPrincipal() {
			return principal;
		}
	}

	public record DummyWithId(String id) {

		public String getId() {
			return id;
		}
	}
}
