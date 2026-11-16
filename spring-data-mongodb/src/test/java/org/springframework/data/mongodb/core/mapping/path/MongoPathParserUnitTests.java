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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.AllPositional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Anchor;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.DbRefField;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Digits;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.FilteredPositional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Name;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Positional;

/**
 * Unit tests for {@link MongoPathParser}.
 *
 * @author Christoph Strobl
 */
class MongoPathParserUnitTests {

	@Test // GH-4516
	void splitsOnDotOnly() {

		MongoPath path = MongoPath.parse("address_line.streetName.0", PathType.QUERY);

		assertThat(path.anchor()).isEqualTo(Anchor.ROOT);
		assertThat(path.tokens()).containsExactly(new Name("address_line"), new Name("streetName"), new Digits("0"));
		assertThat(path).hasToString("address_line.streetName.0");
	}

	@Test // GH-4516
	void retainsLeadingZerosOfDigits() {
		assertThat(MongoPath.parse("a.007", PathType.QUERY).tokens()).containsExactly(new Name("a"), new Digits("007"));
	}

	@Test // GH-4516
	void doesNotTreatNonAsciiDigitsAsDigits() {
		assertThat(MongoPath.parse("a.١", PathType.QUERY).tokens()).containsExactly(new Name("a"), new Name("١"));
	}

	@Test // GH-4516
	void recognizesPositionalInQuery() {

		assertThat(MongoPath.parse("a.$.b", PathType.QUERY).tokens()).containsExactly(new Name("a"), new Positional(),
				new Name("b"));
		assertThat(MongoPath.parse("a.$[].b", PathType.QUERY).tokens()).containsExactly(new Name("a"), new Name("$[]"),
				new Name("b"));
	}

	@Test // GH-4516
	void recognizesAllPositionalOperatorsInUpdate() {

		assertThat(MongoPath.parse("a.$.b.$[].c.$[elem1].d", PathType.UPDATE).tokens()).containsExactly(new Name("a"),
				new Positional(), new Name("b"), new AllPositional(), new Name("c"), new FilteredPositional("elem1"),
				new Name("d"));
	}

	@Test // GH-4516
	void acceptsFilterIdentifiersLeniently() {

		assertThat(MongoPath.parse("a.$[some_item]", PathType.UPDATE).tokens()).containsExactly(new Name("a"),
				new FilteredPositional("some_item"));
		assertThat(MongoPath.parse("a.$[[x]]", PathType.UPDATE).tokens()).containsExactly(new Name("a"),
				new Name("$[[x]]"));
	}

	@Test // GH-4516
	void doesNotTreatLeadingDollarAsPositional() {

		assertThat(MongoPath.parse("$", PathType.UPDATE).tokens()).containsExactly(new Name("$"));
		assertThat(MongoPath.parse("ref.$id", PathType.QUERY).tokens()).containsExactly(new Name("ref"), new Name("$id"));
	}

	@Test // GH-4516
	void parsesAggregationFieldReference() {

		MongoPath path = MongoPath.parse("$a.b", PathType.AGGREGATION);

		assertThat(path.anchor()).isEqualTo(Anchor.FIELD_REFERENCE);
		assertThat(path.tokens()).containsExactly(new Name("a"), new Name("b"));
		assertThat(path).hasToString("$a.b");
	}

	@Test // GH-4516
	void parsesAggregationVariable() {

		MongoPath path = MongoPath.parse("$$this.a", PathType.AGGREGATION);

		assertThat(path.anchor()).isEqualTo(Anchor.variable("this"));
		assertThat(path.tokens()).containsExactly(new Name("a"));
		assertThat(path).hasToString("$$this.a");

		MongoPath root = MongoPath.parse("$$ROOT", PathType.AGGREGATION);
		assertThat(root.anchor()).isEqualTo(Anchor.variable("ROOT"));
		assertThat(root.tokens()).isEmpty();
	}

	@Test // GH-4516
	void retainsEmptySegmentFollowingVariable() {

		assertThat(MongoPath.parse("$$this.", PathType.AGGREGATION).tokens()).containsExactly(new Name(""));
		assertThat(MongoPath.parse("$$this. ", PathType.AGGREGATION).tokens()).containsExactly(new Name(" "));
	}

	@Test // GH-4516
	void doesNotRecognizePositionalInAggregation() {
		assertThat(MongoPath.parse("a.$", PathType.AGGREGATION).tokens()).containsExactly(new Name("a"), new Name("$"));
	}

	@Test // GH-5011, GH-4516
	void recognizesDbRefFieldsInAggregationOnly() {

		assertThat(MongoPath.parse("ref.$id", PathType.AGGREGATION).tokens()).containsExactly(new Name("ref"),
				new DbRefField("$id"));
		assertThat(MongoPath.parse("ref.$ref", PathType.AGGREGATION).tokens()).containsExactly(new Name("ref"),
				new DbRefField("$ref"));
		assertThat(MongoPath.parse("ref.$db", PathType.AGGREGATION).tokens()).containsExactly(new Name("ref"),
				new DbRefField("$db"));
		assertThat(MongoPath.parse("$ref.$id", PathType.AGGREGATION)).hasToString("$ref.$id");
		assertThat(MongoPath.parse("ref.$id", PathType.UPDATE).tokens()).containsExactly(new Name("ref"), new Name("$id"));
		assertThat(MongoPath.parse("ref.$ref", PathType.QUERY).tokens()).containsExactly(new Name("ref"), new Name("$ref"));
		assertThat(MongoPath.parse("ref.$_id", PathType.AGGREGATION).tokens()).containsExactly(new Name("ref"),
				new Name("$_id"));
	}

	@Test // GH-5011, GH-4516
	void recognizesDbRefFieldsFollowingVariable() {

		MongoPath path = MongoPath.parse("$$ref.$id", PathType.AGGREGATION);

		assertThat(path.tokens()).containsExactly(new DbRefField("$id"));
		assertThat(path.variablePath().tokens()).containsExactly(new Name("ref"), new DbRefField("$id"));
		assertThat(path.variablePath().anchor()).isEqualTo(Anchor.ROOT);
	}

	@Test // GH-4516
	void rejectsInvalidTokens() {

		assertThatIllegalArgumentException().isThrownBy(() -> new Digits("1a"));
		assertThatIllegalArgumentException().isThrownBy(() -> new Digits(""));
		assertThatIllegalArgumentException().isThrownBy(() -> new FilteredPositional(""));
		assertThatIllegalArgumentException().isThrownBy(() -> new FilteredPositional("a]"));
	}

	@Test // GH-4516
	void rejectsInvalidAggregationAnchors() {

		assertThatIllegalArgumentException().isThrownBy(() -> MongoPath.parse("$", PathType.AGGREGATION));
		assertThatIllegalArgumentException().isThrownBy(() -> MongoPath.parse("$$", PathType.AGGREGATION));
		assertThatIllegalArgumentException().isThrownBy(() -> MongoPath.parse("$$.a", PathType.AGGREGATION));
	}

	@Test // GH-4516
	void retainsEmptySegments() {
		assertThat(MongoPath.parse("a..b", PathType.QUERY).tokens()).containsExactly(new Name("a"), new Name(""),
				new Name("b"));
	}

	@Test // GH-4516
	void rejectsTooManySegments() {

		String allowed = "a" + ".a".repeat(MongoPath.MAX_SEGMENTS - 1);

		assertThat(MongoPath.parse(allowed, PathType.QUERY).tokens()).hasSize(MongoPath.MAX_SEGMENTS);
		assertThatIllegalArgumentException().isThrownBy(() -> MongoPath.parse(allowed + ".a", PathType.QUERY));
	}

	@Test // GH-4516
	void parsesLongPathWithoutCaching() {

		String cacheable = "a".repeat(MongoPathParser.MAX_CACHEABLE_PATH_LENGTH);
		String uncached = "b".repeat(MongoPathParser.MAX_CACHEABLE_PATH_LENGTH + 1) + ".c";

		assertThat(MongoPath.parse(cacheable, PathType.QUERY)).isSameAs(MongoPath.parse(cacheable, PathType.QUERY));
		assertThat(MongoPath.parse(uncached, PathType.QUERY)).isNotSameAs(MongoPath.parse(uncached, PathType.QUERY))
				.isEqualTo(MongoPath.parse(uncached, PathType.QUERY));
		assertThat(MongoPath.parse(uncached, PathType.QUERY).tokens()).hasSize(2);
	}

	@Test // GH-4516
	void appendsPaths() {

		MongoPath path = MongoPath.parse("a.b", PathType.QUERY).append(MongoPath.parse("c", PathType.QUERY));

		assertThat(path).isEqualTo(MongoPath.parse("a.b.c", PathType.QUERY));
	}

	@Test // GH-4516
	void recognizesSyntaxPerPathType() {

		String path = "a.$.$[].$[e].$id";

		assertThat(MongoPath.parse(path, PathType.PROPERTY).tokens()).containsExactly(new Name("a"), new Name("$"),
				new Name("$[]"), new Name("$[e]"), new Name("$id"));
		assertThat(MongoPath.parse(path, PathType.QUERY).tokens()).containsExactly(new Name("a"), new Positional(),
				new Name("$[]"), new Name("$[e]"), new Name("$id"));
		assertThat(MongoPath.parse(path, PathType.UPDATE).tokens()).containsExactly(new Name("a"), new Positional(),
				new AllPositional(), new FilteredPositional("e"), new Name("$id"));
		assertThat(MongoPath.parse(path, PathType.AGGREGATION).tokens()).containsExactly(new Name("a"), new Name("$"),
				new Name("$[]"), new Name("$[e]"), new DbRefField("$id"));
	}

	@Test // GH-4516
	void recognizesNoOperatorsInPropertyPath() {

		assertThat(MongoPath.parse("a.$.b.$[].c.$[e].$id", PathType.PROPERTY).tokens()).containsExactly(new Name("a"),
				new Name("$"), new Name("b"), new Name("$[]"), new Name("c"), new Name("$[e]"), new Name("$id"));
		assertThat(MongoPath.parse("$a", PathType.PROPERTY).anchor()).isEqualTo(Anchor.ROOT);
	}

	@Test // GH-4516
	void carriesPathType() {

		assertThat(MongoPath.parse("a.b", PathType.UPDATE).type()).isEqualTo(PathType.UPDATE);
		assertThat(MongoPath.name("a.b").type()).isEqualTo(PathType.PROPERTY);
		assertThat(MongoPath.parse("a.b", PathType.QUERY)).isNotEqualTo(MongoPath.parse("a.b", PathType.UPDATE));
	}

	@Test // GH-4516
	void appendsPropertyPathToAnyType() {

		MongoPath path = MongoPath.parse("$a", PathType.AGGREGATION).append(MongoPath.name("b.c"));

		assertThat(path.type()).isEqualTo(PathType.AGGREGATION);
		assertThat(path).hasToString("$a.b.c");
	}

	@Test // GH-4516
	void rejectsAppendingPathOfOtherType() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> MongoPath.parse("a", PathType.QUERY).append(MongoPath.parse("$", PathType.UPDATE)));
	}
}
