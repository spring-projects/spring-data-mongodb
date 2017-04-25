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

import java.util.Locale;
import java.util.Optional;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

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
 * @since 2.0
 * @see <a href="https://docs.mongodb.com/manual/reference/collation/">MongoDB Reference - Collation</a>
 */
public class Collation {

	private static final Collation DEFAULT = of("simple");

	private final ICULocale locale;

	private Optional<ICUComparisonLevel> strength = Optional.empty();
	private Optional<Boolean> numericOrdering = Optional.empty();
	private Optional<Alternate> alternate = Optional.empty();
	private Optional<Boolean> backwards = Optional.empty();
	private Optional<Boolean> normalization = Optional.empty();
	private Optional<String> version = Optional.empty();

	private Collation(ICULocale locale) {

		Assert.notNull(locale, "ICULocale must not be null!");
		this.locale = locale;
	}

	/**
	 * Create new {@link Collation} using simple binary comparison.
	 *
	 * @return
	 * @see #binary()
	 */
	public static Collation simple() {
		return binary();
	}

	/**
	 * Create new {@link Collation} using simple binary comparison.
	 *
	 * @return
	 */
	public static Collation binary() {
		return DEFAULT;
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
		return of(ICULocale.of(locale.getLanguage()).variant(locale.getVariant()));
	}

	/**
	 * Create new {@link Collation} with locale set to the given ICU language.
	 *
	 * @param language must not be {@literal null}.
	 * @return
	 */
	public static Collation of(String language) {
		return of(ICULocale.of(language));
	}

	/**
	 * Create new {@link Collation} with locale set to the given {@link ICULocale}.
	 *
	 * @param locale must not be {@literal null}.
	 * @return
	 */
	public static Collation of(ICULocale locale) {
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
	 * @param strength must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation strength(Integer strength) {

		ICUComparisonLevel current = this.strength.orElseGet(() -> new ICUComparisonLevel(strength, null, null));
		return strength(new ICUComparisonLevel(strength, current.caseFirst.orElse(null), current.caseLevel.orElse(null)));
	}

	/**
	 * Set the level of comparison to perform.
	 *
	 * @param comparisonLevel must not be {@literal null}.
	 * @return new {@link Collation}
	 */
	public Collation strength(ICUComparisonLevel comparisonLevel) {

		Collation newInstance = copy();
		newInstance.strength = Optional.ofNullable(comparisonLevel);
		return newInstance;
	}

	/**
	 * Set {@code caseLevel} comarison. <br />
	 *
	 * @param caseLevel must not be {@literal null}.
	 * @return new {@link Collation}.
	 */
	public Collation caseLevel(Boolean caseLevel) {

		ICUComparisonLevel strengthValue = strength.orElseGet(() -> ICUComparisonLevel.primary());
		return strength(new ICUComparisonLevel(strengthValue.level, strengthValue.caseFirst.orElse(null), caseLevel));
	}

	/**
	 * Set the flag that determines sort order of case differences during tertiary level comparisons.
	 *
	 * @param caseFirst must not be {@literal null}.
	 * @return
	 */
	public Collation caseFirst(String caseFirst) {
		return caseFirst(new ICUCaseFirst(caseFirst));
	}

	/**
	 * Set the flag that determines sort order of case differences during tertiary level comparisons.
	 *
	 * @param caseFirst must not be {@literal null}.
	 * @return
	 */
	public Collation caseFirst(ICUCaseFirst sort) {

		ICUComparisonLevel strengthValue = strength.orElseGet(() -> ICUComparisonLevel.tertiary());
		return strength(new ICUComparisonLevel(strengthValue.level, sort, strengthValue.caseLevel.orElse(null)));
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
	public Collation numericOrdering(Boolean flag) {

		Collation newInstance = copy();
		newInstance.numericOrdering = Optional.ofNullable(flag);
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

		Alternate instance = this.alternate.orElseGet(() -> new Alternate(alternate, null));
		return alternate(new Alternate(alternate, instance.maxVariable.orElse(null)));
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

		Alternate alternateValue = alternate.orElseGet(() -> Alternate.shifted());
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
	public static class ICUComparisonLevel {

		protected final Integer level;
		private final Optional<ICUCaseFirst> caseFirst;
		private final Optional<Boolean> caseLevel;

		private ICUComparisonLevel(Integer level, ICUCaseFirst caseFirst, Boolean caseLevel) {

			this.level = level;
			this.caseFirst = Optional.ofNullable(caseFirst);
			this.caseLevel = Optional.ofNullable(caseLevel);
		}

		/**
		 * Primary level of comparison. Collation performs comparisons of the base characters only, ignoring other
		 * differences such as diacritics and case. <br />
		 * The {@code caseLevel} can be set via {@link ComparisonLevelWithCase#caseLevel(Boolean)}.
		 *
		 * @return new {@link ComparisonLevelWithCase}.
		 */
		public static PrimaryICUComparisonLevel primary() {
			return new PrimaryICUComparisonLevel(1, null);
		}

		/**
		 * Scondary level of comparison. Collation performs comparisons up to secondary differences, such as
		 * diacritics.<br />
		 * The {@code caseLevel} can be set via {@link ComparisonLevelWithCase#caseLevel(Boolean)}.
		 *
		 * @return new {@link ComparisonLevelWithCase}.
		 */
		public static SecondaryICUComparisonLevel secondary() {
			return new SecondaryICUComparisonLevel(2, null);
		}

		/**
		 * Tertiary level of comparison. Collation performs comparisons up to tertiary differences, such as case and letter
		 * variants. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ICUComparisonLevel}.
		 */
		public static TertiaryICUComparisonLevel tertiary() {
			return new TertiaryICUComparisonLevel(3, null);
		}

		/**
		 * Quaternary Level. Limited for specific use case to consider punctuation. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ICUComparisonLevel}.
		 */
		public static ICUComparisonLevel quaternary() {
			return new ICUComparisonLevel(4, null, null);
		}

		/**
		 * Identical Level. Limited for specific use case of tie breaker. <br />
		 * The {@code caseLevel} cannot be set for {@link ICUComparisonLevel} above {@code secondary}.
		 *
		 * @return new {@link ICUComparisonLevel}.
		 */
		public static ICUComparisonLevel identical() {
			return new ICUComparisonLevel(5, null, null);
		}
	}

	public static class TertiaryICUComparisonLevel extends ICUComparisonLevel {

		private TertiaryICUComparisonLevel(Integer level, ICUCaseFirst caseFirst) {
			super(level, caseFirst, null);
		}

		/**
		 * Set the flag that determines sort order of case differences.
		 *
		 * @param caseFirstSort must not be {@literal null}.
		 * @return
		 */
		public TertiaryICUComparisonLevel caseFirst(ICUCaseFirst caseFirst) {

			Assert.notNull(caseFirst, "CaseFirst must not be null!");
			return new TertiaryICUComparisonLevel(level, caseFirst);
		}
	}

	public static class PrimaryICUComparisonLevel extends ICUComparisonLevel {

		private PrimaryICUComparisonLevel(Integer level, Boolean caseLevel) {
			super(level, null, caseLevel);
		}

		/**
		 * Include case comparison.
		 *
		 * @return new {@link ComparisonLevelWithCase}
		 */
		public PrimaryICUComparisonLevel includeCase() {
			return caseLevel(Boolean.TRUE);
		}

		/**
		 * Exclude case comparison.
		 *
		 * @return new {@link ComparisonLevelWithCase}
		 */
		public PrimaryICUComparisonLevel excludeCase() {
			return caseLevel(Boolean.FALSE);
		}

		PrimaryICUComparisonLevel caseLevel(Boolean caseLevel) {
			return new PrimaryICUComparisonLevel(level, caseLevel);
		}
	}

	public static class SecondaryICUComparisonLevel extends ICUComparisonLevel {

		private SecondaryICUComparisonLevel(Integer level, Boolean caseLevel) {
			super(level, null, caseLevel);
		}

		/**
		 * Include case comparison.
		 *
		 * @return new {@link ComparisonLevelWithCase}
		 */
		public SecondaryICUComparisonLevel includeCase() {
			return caseLevel(Boolean.TRUE);
		}

		/**
		 * Exclude case comparison.
		 *
		 * @return new {@link ComparisonLevelWithCase}
		 */
		public SecondaryICUComparisonLevel excludeCase() {
			return caseLevel(Boolean.FALSE);
		}

		SecondaryICUComparisonLevel caseLevel(Boolean caseLevel) {
			return new SecondaryICUComparisonLevel(level, caseLevel);
		}
	}

	/**
	 * @since 2.0
	 */
	public static class ICUCaseFirst {

		private final String state;

		private ICUCaseFirst(String state) {
			this.state = state;
		}

		/**
		 * Sort uppercase before lowercase.
		 *
		 * @return new {@link ICUCaseFirst}.
		 */
		public static ICUCaseFirst upper() {
			return new ICUCaseFirst("upper");
		}

		/**
		 * Sort lowercase before uppercase.
		 *
		 * @return new {@link ICUCaseFirst}.
		 */
		public static ICUCaseFirst lower() {
			return new ICUCaseFirst("lower");
		}

		/**
		 * Use the default.
		 *
		 * @return new {@link ICUCaseFirst}.
		 */
		public static ICUCaseFirst off() {
			return new ICUCaseFirst("off");
		}
	}

	/**
	 * @since 2.0
	 */
	public static class Alternate {

		protected final String alternate;
		protected Optional<String> maxVariable;

		private Alternate(String alternate, String maxVariable) {
			this.alternate = alternate;
			this.maxVariable = Optional.ofNullable(maxVariable);
		}

		/**
		 * Consider Whitespace and punctuation as base characters.
		 *
		 * @return new {@link Alternate}.
		 */
		public static Alternate nonIgnorable() {
			return new Alternate("non-ignorable", null);
		}

		/**
		 * Whitespace and punctuation are <strong>not</strong> considered base characters and are only distinguished at
		 * strength. <br />
		 * <strong>NOTE:</strong> Only works for {@link ICUComparisonLevel} above {@link ICUComparisonLevel#tertiary()}.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public static AlternateWithMaxVariable shifted() {
			return new AlternateWithMaxVariable("shifted", null);
		}
	}

	/**
	 * @since 2.0
	 */
	public static class AlternateWithMaxVariable extends Alternate {

		private AlternateWithMaxVariable(String alternate, String maxVariable) {
			super(alternate, maxVariable);
		}

		/**
		 * Consider both whitespaces and punctuation as ignorable.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public AlternateWithMaxVariable punct() {
			return new AlternateWithMaxVariable(alternate, "punct");
		}

		/**
		 * Only consider whitespaces as ignorable.
		 *
		 * @return new {@link AlternateWithMaxVariable}.
		 */
		public AlternateWithMaxVariable space() {
			return new AlternateWithMaxVariable(alternate, "space");
		}

	}

	/**
	 * ICU locale abstraction for usage with MongoDB {@link Collation}.
	 *
	 * @since 2.0
	 * @see <a href="http://site.icu-project.org">ICU - International Components for Unicode</a>
	 */
	public static class ICULocale {

		private final String language;
		private final Optional<String> variant;

		private ICULocale(String language, String variant) {
			this.language = language;
			this.variant = Optional.ofNullable(variant);
		}

		/**
		 * Create new {@link ICULocale} for given language.
		 *
		 * @param language must not be {@literal null}.
		 * @return
		 */
		public static ICULocale of(String language) {

			Assert.notNull(language, "Code must not be null!");
			return new ICULocale(language, null);
		}

		/**
		 * Define language variant.
		 *
		 * @param variant must not be {@literal null}.
		 * @return new {@link ICULocale}.
		 */
		public ICULocale variant(String variant) {

			Assert.notNull(variant, "Variant must not be null!");
			return new ICULocale(language, variant);
		}

		/**
		 * Get the string representation.
		 *
		 * @return
		 */
		public String asString() {

			StringBuilder sb = new StringBuilder(language);
			variant.ifPresent(val -> {

				if (!val.isEmpty()) {
					sb.append("@collation=").append(val);
				}
			});
			return sb.toString();
		}
	}

	private static Converter<Collation, Document> toMongoDocumentConverter() {

		return source -> {

			Document document = new Document();
			document.append("locale", source.locale.asString());

			source.strength.ifPresent(val -> {

				document.append("strength", val.level);

				val.caseLevel.ifPresent(cl -> document.append("caseLevel", cl));
				val.caseFirst.ifPresent(cl -> document.append("caseFirst", cl.state));
			});

			source.numericOrdering.ifPresent(val -> document.append("numericOrdering", val));
			source.alternate.ifPresent(val -> {

				document.append("alternate", val.alternate);
				val.maxVariable.ifPresent(maxVariable -> document.append("maxVariable", maxVariable));
			});

			source.backwards.ifPresent(val -> document.append("backwards", val));
			source.normalization.ifPresent(val -> document.append("normalization", val));
			source.version.ifPresent(val -> document.append("version", val));

			return document;
		};
	}

	private static Converter<Collation, com.mongodb.client.model.Collation> toMongoCollationConverter() {

		return source -> {

			Builder builder = com.mongodb.client.model.Collation.builder();

			builder.locale(source.locale.asString());

			source.strength.ifPresent(val -> {

				builder.collationStrength(CollationStrength.fromInt(val.level));

				val.caseLevel.ifPresent(cl -> builder.caseLevel(cl));
				val.caseFirst.ifPresent(cl -> builder.collationCaseFirst(CollationCaseFirst.fromString(cl.state)));
			});

			source.numericOrdering.ifPresent(val -> builder.numericOrdering(val));
			source.alternate.ifPresent(val -> {

				builder.collationAlternate(CollationAlternate.fromString(val.alternate));
				val.maxVariable
						.ifPresent(maxVariable -> builder.collationMaxVariable(CollationMaxVariable.fromString(maxVariable)));
			});

			source.backwards.ifPresent(val -> builder.backwards(val));
			source.normalization.ifPresent(val -> builder.normalization(val));

			return builder.build();
		};
	}
}
