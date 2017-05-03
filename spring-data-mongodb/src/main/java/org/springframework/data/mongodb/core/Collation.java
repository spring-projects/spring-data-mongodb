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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Locale;
import java.util.Optional;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.model.Collation.Builder;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;

/**
 * Central abstraction for MongoDB collation support. <br />
 * Allows fluent creation of a collation {@link Document} that can be used for creating collections & indexes as well as
 * querying data.
 * <p />
 * <strong>NOTE:</strong> Please keep in mind that queries will only make use of an index with collation settings if the
 * query itself specifies the same collation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 * @see <a href="https://docs.mongodb.com/manual/reference/collation/">MongoDB Reference - Collation</a>
 */
public class Collation {

	private static final Collation SIMPLE = of("simple");

	private final CollationLocale locale;

	private Optional<ComparisonLevel> strength = Optional.empty();
	private Optional<Boolean> numericOrdering = Optional.empty();
	private Optional<Alternate> alternate = Optional.empty();
	private Optional<Boolean> backwards = Optional.empty();
	private Optional<Boolean> normalization = Optional.empty();
	private Optional<String> version = Optional.empty();

	private Collation(CollationLocale locale) {

		Assert.notNull(locale, "ICULocale must not be null!");
		this.locale = locale;
	}

	/**
	 * Create a {@link Collation} using {@literal simple} binary comparison.
	 *
	 * @return a {@link Collation} for {@literal simple} binary comparison.
	 */
	public static Collation simple() {
		return SIMPLE;
	}

	/**
	 * Create new {@link Collation} with locale set to {{@link java.util.Locale#getLanguage()}} and
	 * {@link java.util.Locale#getVariant()}.
	 *
	 * @param locale must not be {@literal null}.
	 * @return
	 */
	public static Collation of(Locale locale) {

		Assert.notNull(locale, "Locale must not be null!");

		String format;

		if (StringUtils.hasText(locale.getCountry())) {
			format = String.format("%s_%s", locale.getLanguage(), locale.getCountry());
		} else {
			format = locale.getLanguage();
		}

		return of(CollationLocale.of(format).variant(locale.getVariant()));
	}

	/**
	 * Create new {@link Collation} with locale set to the given ICU language.
	 *
	 * @param language must not be {@literal null}.
	 * @return
	 */
	public static Collation of(String language) {
		return of(CollationLocale.of(language));
	}

	/**
	 * Create new {@link Collation} with locale set to the given {@link CollationLocale}.
	 *
	 * @param locale must not be {@literal null}.
	 * @return
	 */
	public static Collation of(CollationLocale locale) {
		return new Collation(locale);
	}

	/**
	 * Create new {@link Collation} from values in {@link Document}.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 * @see <a href="https://docs.mongodb.com/manual/reference/collation/#collation-document">MongoDB Reference -
	 *      Collation Document</a>
	 */
	public static Collation from(Document source) {

		Assert.notNull(source, "Source must not be null!");

		Collation collation = Collation.of(source.getString("locale"));
		if (source.containsKey("strength")) {
			collation = collation.strength(source.getInteger("strength"));
		}
		if (source.containsKey("caseLevel")) {
			collation = collation.caseLevel(source.getBoolean("caseLevel"));
		}
		if (source.containsKey("caseFirst")) {
			collation = collation.caseFirst(source.getString("caseFirst"));
		}
		if (source.containsKey("numericOrdering")) {
			collation = collation.numericOrdering(source.getBoolean("numericOrdering"));
		}
		if (source.containsKey("alternate")) {
			collation = collation.alternate(source.getString("alternate"));
		}
		if (source.containsKey("maxVariable")) {
			collation = collation.maxVariable(source.getString("maxVariable"));
		}
		if (source.containsKey("backwards")) {
			collation = collation.backwards(source.getBoolean("backwards"));
		}
		if (source.containsKey("normalization")) {
			collation = collation.normalization(source.getBoolean("normalization"));
		}
		if (source.containsKey("version")) {
			collation.version = Optional.of(source.get("version").toString());
		}
		return collation;
	}

	/**
	 * Set the level of comparison to perform.
	 *
	 * @param strength
	 * @return new {@link Collation}.
	 */
	public Collation strength(int strength) {

		ComparisonLevel current = this.strength.orElseGet(() -> new ICUComparisonLevel(strength));
		return strength(new ICUComparisonLevel(strength, current.getCaseFirst(), current.getCaseLevel()));
	}

	/**
	 * Set the level of comparison to perform.
	 *
	 * @param comparisonLevel must not be {@literal null}.
	 * @return new {@link Collation}
	 */
	public Collation strength(ComparisonLevel comparisonLevel) {

		Collation newInstance = copy();
		newInstance.strength = Optional.of(comparisonLevel);
		return newInstance;
	}

	/**
	 * Set whether to include {@code caseLevel} comparison. <br />
	 *
	 * @param caseLevel
	 * @return new {@link Collation}.
	 */
	public Collation caseLevel(boolean caseLevel) {

		ComparisonLevel strengthValue = strength.orElseGet(ComparisonLevel::primary);
		return strength(
				new ICUComparisonLevel(strengthValue.getLevel(), strengthValue.getCaseFirst(), Optional.of(caseLevel)));
	}

	/**
	 * Set the flag that determines sort order of case differences during tertiary level comparisons.
	 *
	 * @param caseFirst must not be {@literal null}.
	 * @return
	 */
	public Collation caseFirst(String caseFirst) {
		return caseFirst(new CaseFirst(caseFirst));
	}

	/**
	 * Set the flag that determines sort order of case differences during tertiary level comparisons.
	 *
	 * @param caseFirst must not be {@literal null}.
	 * @return
	 */
	public Collation caseFirst(CaseFirst sort) {

		ComparisonLevel strengthValue = strength.orElseGet(ComparisonLevel::tertiary);
		return strength(new ICUComparisonLevel(strengthValue.getLevel(), Optional.of(sort), strengthValue.getCaseLevel()));
	}

	/**
	 * Treat numeric strings as numbers for comparison.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation numericOrderingEnabled() {
		return numericOrdering(true);
	}

	/**
	 * Treat numeric strings as string for comparison.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation numericOrderingDisabled() {
		return numericOrdering(false);
	}

	/**
	 * Set the flag that determines whether to compare numeric strings as numbers or as strings.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation numericOrdering(boolean flag) {

		Collation newInstance = copy();
		newInstance.numericOrdering = Optional.of(flag);
		return newInstance;
	}

	/**
	 * Set the Field that determines whether collation should consider whitespace and punctuation as base characters for
	 * purposes of comparison.
	 *
	 * @param alternate must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation alternate(String alternate) {

		Alternate instance = this.alternate.orElseGet(() -> new Alternate(alternate, Optional.empty()));
		return alternate(new Alternate(alternate, instance.maxVariable));
	}

	/**
	 * Set the Field that determines whether collation should consider whitespace and punctuation as base characters for
	 * purposes of comparison.
	 *
	 * @param alternate must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation alternate(Alternate alternate) {

		Collation newInstance = copy();
		newInstance.alternate = Optional.ofNullable(alternate);
		return newInstance;
	}

	/**
	 * Sort string with diacritics sort from back of the string.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation backwardDiacriticSort() {
		return backwards(true);
	}

	/**
	 * Do not sort string with diacritics sort from back of the string.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation forwardDiacriticSort() {
		return backwards(false);
	}

	/**
	 * Set the flag that determines whether strings with diacritics sort from back of the string.
	 *
	 * @param backwards must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation backwards(Boolean backwards) {

		Collation newInstance = copy();
		newInstance.backwards = Optional.ofNullable(backwards);
		return newInstance;
	}

	/**
	 * Enable text normalization.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation normalizationEnabled() {
		return normalization(true);
	}

	/**
	 * Disable text normalization.
	 *
	 * @return new {@link Collation}.
	 */
	public Collation normalizationDisabled() {
		return normalization(false);
	}

	/**
	 * Set the flag that determines whether to check if text require normalization and to perform normalization.
	 *
	 * @param normalization must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation normalization(Boolean normalization) {

		Collation newInstance = copy();
		newInstance.normalization = Optional.ofNullable(normalization);
		return newInstance;
	}

	/**
	 * Set the field that determines up to which characters are considered ignorable when alternate is {@code shifted}.
	 *
	 * @param maxVariable must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation maxVariable(String maxVariable) {

		Alternate alternateValue = alternate.orElseGet(Alternate::shifted);
		return alternate(new AlternateWithMaxVariable(alternateValue.alternate, maxVariable));
	}

	/**
	 * Get the {@link Document} representation of the {@link Collation}.
	 *
	 * @return
	 */
	public Document toDocument() {
		return map(toMongoDocumentConverter());
	}

	/**
	 * Get the {@link com.mongodb.client.model.Collation} representation of the {@link Collation}.
	 *
	 * @return
	 */
	public com.mongodb.client.model.Collation toMongoCollation() {
		return map(toMongoCollationConverter());
	}

	/**
	 * Transform {@code this} {@link Collation} by applying a {@link Converter}.
	 *
	 * @param mapper
	 * @param <R>
	 * @return
	 */
	public <R> R map(Converter<? super Collation, ? extends R> mapper) {
		return mapper.convert(this);
	}

	@Override
	public String toString() {
		return toDocument().toJson();
	}

	private Collation copy() {

		Collation collation = new Collation(locale);
		collation.strength = this.strength;
		collation.normalization = this.normalization;
		collation.numericOrdering = this.numericOrdering;
		collation.alternate = this.alternate;
		collation.backwards = this.backwards;
		return collation;
	}

	/**
	 * Abstraction for the ICU Comparison Levels.
	 *
	 * @since 2.0
	 */
	public interface ComparisonLevel {

		/**
		 * Primary level of comparison. Collation performs comparisons of the base characters only, ignoring other
		 * differences such as diacritics and case. <br />
		 * The {@code caseLevel} can be set via {@link PrimaryICUComparisonLevel#includeCase()} and
		 * {@link PrimaryICUComparisonLevel#excludeCase()}.
		 *
		 * @return new {@link SecondaryICUComparisonLevel}.
		 */
		static PrimaryICUComparisonLevel primary() {
			return PrimaryICUComparisonLevel.DEFAULT;
		}

		/**
		 * Secondary level of comparison. Collation performs comparisons up to secondary differences, such as
		 * diacritics.<br />
		 * The {@code caseLevel} can be set via {@link SecondaryICUComparisonLevel#includeCase()} and
		 * {@link SecondaryICUComparisonLevel#excludeCase()}.
		 *
		 * @return new {@link SecondaryICUComparisonLevel}.
		 */
		static SecondaryICUComparisonLevel secondary() {
			return SecondaryICUComparisonLevel.DEFAULT;
		}

		/**
		 * Tertiary level of comparison. Collation performs comparisons up to tertiary differences, such as case and letter
		 * variants. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ICUComparisonLevel}.
		 */
		static TertiaryICUComparisonLevel tertiary() {
			return TertiaryICUComparisonLevel.DEFAULT;
		}

		/**
		 * Quaternary Level. Limited for specific use case to consider punctuation. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ComparisonLevel}.
		 */
		static ComparisonLevel quaternary() {
			return ComparisonLevels.QUATERNARY;
		}

		/**
		 * Identical Level. Limited for specific use case of tie breaker. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ComparisonLevel}.
		 */
		static ComparisonLevel identical() {
			return ComparisonLevels.IDENTICAL;
		}

		/**
		 * @return collation strength, {@literal 1} for primary, {@literal 2} for secondary and so on.
		 */
		int getLevel();

		default Optional<CaseFirst> getCaseFirst() {
			return Optional.empty();
		}

		default Optional<Boolean> getCaseLevel() {
			return Optional.empty();
		}
	}

	/**
	 * Abstraction for the ICU Comparison Levels.
	 *
	 * @since 2.0
	 */
	@AllArgsConstructor(access = AccessLevel.PACKAGE)
	@Getter
	static class ICUComparisonLevel implements ComparisonLevel {

		private final int level;
		private final Optional<CaseFirst> caseFirst;
		private final Optional<Boolean> caseLevel;

		ICUComparisonLevel(int level) {
			this(level, Optional.empty(), Optional.empty());
		}
	}

	/**
	 * Simple comparison levels.
	 */
	enum ComparisonLevels implements ComparisonLevel {

		QUATERNARY(4), IDENTICAL(5);

		private final int level;

		ComparisonLevels(int level) {
			this.level = level;
		}

		@Override
		public int getLevel() {
			return level;
		}
	}

	/**
	 * Primary-strength {@link ICUComparisonLevel}.
	 */
	public static class PrimaryICUComparisonLevel extends ICUComparisonLevel {

		static final PrimaryICUComparisonLevel DEFAULT = new PrimaryICUComparisonLevel();
		static final PrimaryICUComparisonLevel WITH_CASE_LEVEL = new PrimaryICUComparisonLevel(true);
		static final PrimaryICUComparisonLevel WITHOUT_CASE_LEVEL = new PrimaryICUComparisonLevel(false);

		private PrimaryICUComparisonLevel() {
			super(1);
		}

		private PrimaryICUComparisonLevel(boolean caseLevel) {
			super(1, Optional.empty(), Optional.of(caseLevel));
		}

		/**
		 * Include case comparison.
		 *
		 * @return new {@link ICUComparisonLevel}
		 */
		public ComparisonLevel includeCase() {
			return WITH_CASE_LEVEL;
		}

		/**
		 * Exclude case comparison.
		 *
		 * @return new {@link ICUComparisonLevel}
		 */
		public ComparisonLevel excludeCase() {
			return WITHOUT_CASE_LEVEL;
		}
	}

	/**
	 * Secondary-strength {@link ICUComparisonLevel}.
	 */
	public static class SecondaryICUComparisonLevel extends ICUComparisonLevel {

		static final SecondaryICUComparisonLevel DEFAULT = new SecondaryICUComparisonLevel();
		static final SecondaryICUComparisonLevel WITH_CASE_LEVEL = new SecondaryICUComparisonLevel(true);
		static final SecondaryICUComparisonLevel WITHOUT_CASE_LEVEL = new SecondaryICUComparisonLevel(false);

		private SecondaryICUComparisonLevel() {
			super(2);
		}

		private SecondaryICUComparisonLevel(boolean caseLevel) {
			super(2, Optional.empty(), Optional.of(caseLevel));
		}

		/**
		 * Include case comparison.
		 *
		 * @return new {@link SecondaryICUComparisonLevel}
		 */
		public ComparisonLevel includeCase() {
			return WITH_CASE_LEVEL;
		}

		/**
		 * Exclude case comparison.
		 *
		 * @return new {@link SecondaryICUComparisonLevel}
		 */
		public ComparisonLevel excludeCase() {
			return WITHOUT_CASE_LEVEL;
		}
	}

	/**
	 * Tertiary-strength {@link ICUComparisonLevel}.
	 */
	public static class TertiaryICUComparisonLevel extends ICUComparisonLevel {

		static final TertiaryICUComparisonLevel DEFAULT = new TertiaryICUComparisonLevel();

		private TertiaryICUComparisonLevel() {
			super(3);
		}

		private TertiaryICUComparisonLevel(CaseFirst caseFirst) {
			super(3, Optional.of(caseFirst), Optional.empty());
		}

		/**
		 * Set the flag that determines sort order of case differences.
		 *
		 * @param caseFirst must not be {@literal null}.
		 * @return new {@link ICUComparisonLevel}
		 */
		public ComparisonLevel caseFirst(CaseFirst caseFirst) {

			Assert.notNull(caseFirst, "CaseFirst must not be null!");
			return new TertiaryICUComparisonLevel(caseFirst);
		}
	}

	/**
	 * @since 2.0
	 */
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	public static class CaseFirst {

		private static final CaseFirst UPPER = new CaseFirst("upper");
		private static final CaseFirst LOWER = new CaseFirst("lower");
		private static final CaseFirst OFF = new CaseFirst("off");

		private final String state;

		/**
		 * Sort uppercase before lowercase.
		 *
		 * @return new {@link CaseFirst}.
		 */
		public static CaseFirst upper() {
			return UPPER;
		}

		/**
		 * Sort lowercase before uppercase.
		 *
		 * @return new {@link CaseFirst}.
		 */
		public static CaseFirst lower() {
			return LOWER;
		}

		/**
		 * Use the default.
		 *
		 * @return new {@link CaseFirst}.
		 */
		public static CaseFirst off() {
			return OFF;
		}
	}

	/**
	 * @since 2.0
	 */
	@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
	public static class Alternate {

		private static final Alternate NON_IGNORABLE = new Alternate("non-ignorable", Optional.empty());

		final String alternate;
		final Optional<String> maxVariable;

		/**
		 * Consider Whitespace and punctuation as base characters.
		 *
		 * @return new {@link Alternate}.
		 */
		public static Alternate nonIgnorable() {
			return NON_IGNORABLE;
		}

		/**
		 * Whitespace and punctuation are <strong>not</strong> considered base characters and are only distinguished at
		 * strength. <br />
		 * <strong>NOTE:</strong> Only works for {@link ICUComparisonLevel} above {@link ComparisonLevel#tertiary()}.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public static AlternateWithMaxVariable shifted() {
			return AlternateWithMaxVariable.DEFAULT;
		}
	}

	/**
	 * @since 2.0
	 */
	public static class AlternateWithMaxVariable extends Alternate {

		static final AlternateWithMaxVariable DEFAULT = new AlternateWithMaxVariable("shifted");
		static final Alternate SHIFTED_PUNCT = new AlternateWithMaxVariable("shifted", "punct");
		static final Alternate SHIFTED_SPACE = new AlternateWithMaxVariable("shifted", "space");

		private AlternateWithMaxVariable(String alternate) {
			super(alternate, Optional.empty());
		}

		private AlternateWithMaxVariable(String alternate, String maxVariable) {
			super(alternate, Optional.of(maxVariable));
		}

		/**
		 * Consider both whitespaces and punctuation as ignorable.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public Alternate punct() {
			return SHIFTED_PUNCT;
		}

		/**
		 * Only consider whitespaces as ignorable.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public Alternate space() {
			return SHIFTED_SPACE;
		}
	}

	/**
	 * ICU locale abstraction for usage with MongoDB {@link Collation}.
	 *
	 * @since 2.0
	 * @see <a href="http://site.icu-project.org">ICU - International Components for Unicode</a>
	 */
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	public static class CollationLocale {

		private final String language;
		private final Optional<String> variant;

		/**
		 * Create new {@link CollationLocale} for given language.
		 *
		 * @param language must not be {@literal null}.
		 * @return
		 */
		public static CollationLocale of(String language) {

			Assert.notNull(language, "Code must not be null!");
			return new CollationLocale(language, Optional.empty());
		}

		/**
		 * Define language variant.
		 *
		 * @param variant must not be {@literal null}.
		 * @return new {@link CollationLocale}.
		 */
		public CollationLocale variant(String variant) {

			Assert.notNull(variant, "Variant must not be null!");
			return new CollationLocale(language, Optional.of(variant));
		}

		/**
		 * Get the string representation.
		 *
		 * @return
		 */
		public String asString() {

			StringBuilder sb = new StringBuilder(language);

			variant.filter(it -> !it.isEmpty()).ifPresent(val -> {

				// Mongo requires variant rendered as ICU keyword (@key=value;key=value…)
				sb.append("@collation=").append(val);
			});

			return sb.toString();
		}
	}

	private static Converter<Collation, Document> toMongoDocumentConverter() {

		return source -> {

			Document document = new Document();
			document.append("locale", source.locale.asString());

			source.strength.ifPresent(strength -> {

				document.append("strength", strength.getLevel());

				strength.getCaseLevel().ifPresent(it -> document.append("caseLevel", it));
				strength.getCaseFirst().ifPresent(it -> document.append("caseFirst", it.state));
			});

			source.numericOrdering.ifPresent(val -> document.append("numericOrdering", val));
			source.alternate.ifPresent(it -> {

				document.append("alternate", it.alternate);
				it.maxVariable.ifPresent(maxVariable -> document.append("maxVariable", maxVariable));
			});

			source.backwards.ifPresent(it -> document.append("backwards", it));
			source.normalization.ifPresent(it -> document.append("normalization", it));
			source.version.ifPresent(it -> document.append("version", it));

			return document;
		};
	}

	private static Converter<Collation, com.mongodb.client.model.Collation> toMongoCollationConverter() {

		return source -> {

			Builder builder = com.mongodb.client.model.Collation.builder();

			builder.locale(source.locale.asString());

			source.strength.ifPresent(strength -> {

				builder.collationStrength(CollationStrength.fromInt(strength.getLevel()));

				strength.getCaseLevel().ifPresent(builder::caseLevel);
				strength.getCaseFirst().ifPresent(it -> builder.collationCaseFirst(CollationCaseFirst.fromString(it.state)));
			});

			source.numericOrdering.ifPresent(builder::numericOrdering);
			source.alternate.ifPresent(it -> {

				builder.collationAlternate(CollationAlternate.fromString(it.alternate));
				it.maxVariable
						.ifPresent(maxVariable -> builder.collationMaxVariable(CollationMaxVariable.fromString(maxVariable)));
			});

			source.backwards.ifPresent(builder::backwards);
			source.normalization.ifPresent(builder::normalization);

			return builder.build();
		};
	}
}
