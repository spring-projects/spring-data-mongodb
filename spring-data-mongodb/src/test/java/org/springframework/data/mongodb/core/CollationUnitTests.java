/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Locale;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.Collation.Alternate;
import org.springframework.data.mongodb.core.Collation.CaseFirst;
import org.springframework.data.mongodb.core.Collation.CollationLocale;
import org.springframework.data.mongodb.core.Collation.ComparisonLevel;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class CollationUnitTests {

	static final Document BINARY_COMPARISON = new Document().append("locale", "simple");
	static final Document JUST_LOCALE = new Document().append("locale", "en_US");
	static final Document LOCALE_WITH_VARIANT = new Document().append("locale", "de_AT@collation=phonebook");
	static final Document WITH_STRENGTH_PRIMARY = new Document(JUST_LOCALE).append("strength", 1);
	static final Document WITH_STRENGTH_PRIMARY_INCLUDE_CASE = new Document(WITH_STRENGTH_PRIMARY).append("caseLevel",
			true);
	static final Document WITH_NORMALIZATION = new Document(JUST_LOCALE).append("normalization", true);
	static final Document WITH_BACKWARDS = new Document(JUST_LOCALE).append("backwards", true);
	static final Document WITH_NUMERIC_ORDERING = new Document(JUST_LOCALE).append("numericOrdering", true);
	static final Document WITH_CASE_FIRST_UPPER = new Document(JUST_LOCALE).append("strength", 3).append("caseFirst",
			"upper");
	static final Document WITH_ALTERNATE_SHIFTED = new Document(JUST_LOCALE).append("alternate", "shifted");
	static final Document WITH_ALTERNATE_SHIFTED_MAX_VARIABLE_PUNCT = new Document(WITH_ALTERNATE_SHIFTED)
			.append("maxVariable", "punct");
	static final Document ALL_THE_THINGS = new Document(LOCALE_WITH_VARIANT).append("strength", 1)
			.append("caseLevel", true).append("backwards", true).append("numericOrdering", true)
			.append("alternate", "shifted").append("maxVariable", "punct").append("normalization", true);

	@Test // DATAMONGO-1518
	public void justLocale() {
		assertThat(Collation.of("en_US").toDocument()).isEqualTo(JUST_LOCALE);
	}

	@Test // DATAMONGO-1518
	public void justLocaleFromDocument() {
		assertThat(Collation.from(JUST_LOCALE).toDocument()).isEqualTo(JUST_LOCALE);
	}

	@Test // DATAMONGO-1518
	public void localeWithVariant() {
		assertThat(Collation.of(CollationLocale.of("de_AT").variant("phonebook")).toDocument())
				.isEqualTo(LOCALE_WITH_VARIANT);
	}

	@Test // DATAMONGO-1518
	public void localeWithVariantFromDocument() {
		assertThat(Collation.from(LOCALE_WITH_VARIANT).toDocument()).isEqualTo(LOCALE_WITH_VARIANT);
	}

	@Test // DATAMONGO-1518
	public void localeFromJavaUtilLocale() {

		assertThat(Collation.of(java.util.Locale.US).toDocument()).isEqualTo(new Document().append("locale", "en_US"));
		assertThat(Collation.of(Locale.ENGLISH).toDocument()).isEqualTo(new Document().append("locale", "en"));
	}

	@Test // DATAMONGO-1518
	public void withStrenghPrimary() {
		assertThat(Collation.of("en_US").strength(ComparisonLevel.primary()).toDocument()).isEqualTo(WITH_STRENGTH_PRIMARY);
	}

	@Test // DATAMONGO-1518
	public void withStrenghPrimaryFromDocument() {
		assertThat(Collation.from(WITH_STRENGTH_PRIMARY).toDocument()).isEqualTo(WITH_STRENGTH_PRIMARY);
	}

	@Test // DATAMONGO-1518
	public void withStrenghPrimaryAndIncludeCase() {

		assertThat(Collation.of("en_US").strength(ComparisonLevel.primary().includeCase()).toDocument())
				.isEqualTo(WITH_STRENGTH_PRIMARY_INCLUDE_CASE);
	}

	@Test // DATAMONGO-1518
	public void withStrenghPrimaryAndIncludeCaseFromDocument() {

		assertThat(Collation.from(WITH_STRENGTH_PRIMARY_INCLUDE_CASE).toDocument())
				.isEqualTo(WITH_STRENGTH_PRIMARY_INCLUDE_CASE);
	}

	@Test // DATAMONGO-1518
	public void withNormalization() {
		assertThat(Collation.of("en_US").normalization(true).toDocument()).isEqualTo(WITH_NORMALIZATION);
	}

	@Test // DATAMONGO-1518
	public void withNormalizationFromDocument() {
		assertThat(Collation.from(WITH_NORMALIZATION).toDocument()).isEqualTo(WITH_NORMALIZATION);
	}

	@Test // DATAMONGO-1518
	public void withBackwards() {
		assertThat(Collation.of("en_US").backwards(true).toDocument()).isEqualTo(WITH_BACKWARDS);
	}

	@Test // DATAMONGO-1518
	public void withBackwardsFromDocument() {
		assertThat(Collation.from(WITH_BACKWARDS).toDocument()).isEqualTo(WITH_BACKWARDS);
	}

	@Test // DATAMONGO-1518
	public void withNumericOrdering() {
		assertThat(Collation.of("en_US").numericOrdering(true).toDocument()).isEqualTo(WITH_NUMERIC_ORDERING);
	}

	@Test // DATAMONGO-1518
	public void withNumericOrderingFromDocument() {
		assertThat(Collation.from(WITH_NUMERIC_ORDERING).toDocument()).isEqualTo(WITH_NUMERIC_ORDERING);
	}

	@Test // DATAMONGO-1518
	public void withCaseFirst() {
		assertThat(Collation.of("en_US").caseFirst(CaseFirst.upper()).toDocument()).isEqualTo(WITH_CASE_FIRST_UPPER);
	}

	@Test // DATAMONGO-1518
	public void withCaseFirstFromDocument() {
		assertThat(Collation.from(WITH_CASE_FIRST_UPPER).toDocument()).isEqualTo(WITH_CASE_FIRST_UPPER);
	}

	@Test // DATAMONGO-1518
	public void withAlternate() {
		assertThat(Collation.of("en_US").alternate(Alternate.shifted()).toDocument()).isEqualTo(WITH_ALTERNATE_SHIFTED);
	}

	@Test // DATAMONGO-1518
	public void withAlternateFromDocument() {
		assertThat(Collation.from(WITH_ALTERNATE_SHIFTED).toDocument()).isEqualTo(WITH_ALTERNATE_SHIFTED);
	}

	@Test // DATAMONGO-1518
	public void withAlternateAndMaxVariable() {

		assertThat(Collation.of("en_US").alternate(Alternate.shifted().punct()).toDocument())
				.isEqualTo(WITH_ALTERNATE_SHIFTED_MAX_VARIABLE_PUNCT);
	}

	@Test // DATAMONGO-1518
	public void withAlternateAndMaxVariableFromDocument() {

		assertThat(Collation.from(WITH_ALTERNATE_SHIFTED_MAX_VARIABLE_PUNCT).toDocument())
				.isEqualTo(WITH_ALTERNATE_SHIFTED_MAX_VARIABLE_PUNCT);
	}

	@Test // DATAMONGO-1518
	public void allTheThings() {

		assertThat(Collation.of(CollationLocale.of("de_AT").variant("phonebook"))
				.strength(ComparisonLevel.primary().includeCase()).normalizationEnabled().backwardDiacriticSort()
				.numericOrderingEnabled().alternate(Alternate.shifted().punct()).toDocument()).isEqualTo(ALL_THE_THINGS);
	}

	@Test // DATAMONGO-1518
	public void allTheThingsFromDocument() {
		assertThat(Collation.from(ALL_THE_THINGS).toDocument()).isEqualTo(ALL_THE_THINGS);
	}

	@Test // DATAMONGO-1518
	public void justTheDefault() {
		assertThat(Collation.simple().toDocument()).isEqualTo(BINARY_COMPARISON);
	}

}
