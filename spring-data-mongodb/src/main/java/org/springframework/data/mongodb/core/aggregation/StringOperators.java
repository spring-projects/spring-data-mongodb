/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.domain.Range;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal String} aggregation operations.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.10
 */
public class StringOperators {

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static StringOperatorFactory valueOf(String fieldReference) {
		return new StringOperatorFactory(fieldReference);
	}

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static StringOperatorFactory valueOf(AggregationExpression fieldReference) {
		return new StringOperatorFactory(fieldReference);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class StringOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link StringOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public StringOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link StringOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public StringOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats the value
		 * of the referenced field to it.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Concat concatValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createConcat().concatValueOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats the result
		 * of the given {@link AggregationExpression} to it.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Concat concatValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createConcat().concatValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats given
		 * {@literal value} to it.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public Concat concat(String value) {

			Assert.notNull(value, "Value must not be null!");
			return createConcat().concat(value);
		}

		private Concat createConcat() {
			return usesFieldRef() ? Concat.valueOf(fieldReference) : Concat.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified index position.
		 *
		 * @param start
		 * @return
		 */
		public Substr substring(int start) {
			return substring(start, -1);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified index position including the specified number of characters.
		 *
		 * @param start
		 * @param nrOfChars
		 * @return
		 */
		public Substr substring(int start, int nrOfChars) {
			return createSubstr().substring(start, nrOfChars);
		}

		private Substr createSubstr() {
			return usesFieldRef() ? Substr.valueOf(fieldReference) : Substr.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and lowers it.
		 *
		 * @return
		 */
		public ToLower toLower() {
			return usesFieldRef() ? ToLower.lowerValueOf(fieldReference) : ToLower.lowerValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and uppers it.
		 *
		 * @return
		 */
		public ToUpper toUpper() {
			return usesFieldRef() ? ToUpper.upperValueOf(fieldReference) : ToUpper.upperValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public StrCaseCmp strCaseCmp(String value) {

			Assert.notNull(value, "Value must not be null!");
			return createStrCaseCmp().strcasecmp(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the referenced {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public StrCaseCmp strCaseCmpValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createStrCaseCmp().strcasecmpValueOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the result of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public StrCaseCmp strCaseCmpValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createStrCaseCmp().strcasecmpValueOf(expression);
		}

		private StrCaseCmp createStrCaseCmp() {
			return usesFieldRef() ? StrCaseCmp.valueOf(fieldReference) : StrCaseCmp.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a given {@literal substring} and returns the UTF-8 byte index (zero-based) of the first
		 * occurrence.
		 *
		 * @param substring must not be {@literal null}.
		 * @return
		 */
		public IndexOfBytes indexOf(String substring) {

			Assert.notNull(substring, "Substring must not be null!");
			return createIndexOfBytesSubstringBuilder().indexOf(substring);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring contained in the given {@literal field reference} and returns the UTF-8 byte
		 * index (zero-based) of the first occurrence.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public IndexOfBytes indexOf(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createIndexOfBytesSubstringBuilder().indexOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring resulting from the given {@link AggregationExpression} and returns the UTF-8
		 * byte index (zero-based) of the first occurrence.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public IndexOfBytes indexOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createIndexOfBytesSubstringBuilder().indexOf(expression);
		}

		private IndexOfBytes.SubstringBuilder createIndexOfBytesSubstringBuilder() {
			return usesFieldRef() ? IndexOfBytes.valueOf(fieldReference) : IndexOfBytes.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a given {@literal substring} and returns the UTF-8 code point index (zero-based) of the
		 * first occurrence.
		 *
		 * @param substring must not be {@literal null}.
		 * @return
		 */
		public IndexOfCP indexOfCP(String substring) {

			Assert.notNull(substring, "Substring must not be null!");
			return createIndexOfCPSubstringBuilder().indexOf(substring);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring contained in the given {@literal field reference} and returns the UTF-8 code
		 * point index (zero-based) of the first occurrence.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public IndexOfCP indexOfCP(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createIndexOfCPSubstringBuilder().indexOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring resulting from the given {@link AggregationExpression} and returns the UTF-8
		 * code point index (zero-based) of the first occurrence.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public IndexOfCP indexOfCP(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createIndexOfCPSubstringBuilder().indexOf(expression);
		}

		private IndexOfCP.SubstringBuilder createIndexOfCPSubstringBuilder() {
			return usesFieldRef() ? IndexOfCP.valueOf(fieldReference) : IndexOfCP.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated string representation into an array of
		 * substrings based on the given delimiter.
		 *
		 * @param delimiter must not be {@literal null}.
		 * @return
		 */
		public Split split(String delimiter) {
			return createSplit().split(delimiter);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated string representation into an array of
		 * substrings based on the delimiter resulting from the referenced field..
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Split split(Field fieldReference) {
			return createSplit().split(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated string representation into an array of
		 * substrings based on a delimiter resulting from the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Split split(AggregationExpression expression) {
			return createSplit().split(expression);
		}

		private Split createSplit() {
			return usesFieldRef() ? Split.valueOf(fieldReference) : Split.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the number of UTF-8 bytes in the associated string
		 * representation.
		 *
		 * @return
		 */
		public StrLenBytes length() {
			return usesFieldRef() ? StrLenBytes.stringLengthOf(fieldReference)
					: StrLenBytes.stringLengthOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the number of UTF-8 code points in the associated string
		 * representation.
		 *
		 * @return
		 */
		public StrLenCP lengthCP() {
			return usesFieldRef() ? StrLenCP.stringLengthOfCP(fieldReference) : StrLenCP.stringLengthOfCP(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified code point index position.
		 *
		 * @param codePointStart
		 * @return
		 */
		public SubstrCP substringCP(int codePointStart) {
			return substringCP(codePointStart, -1);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified code point index position including the specified number of code points.
		 *
		 * @param codePointStart
		 * @param nrOfCodePoints
		 * @return
		 */
		public SubstrCP substringCP(int codePointStart, int nrOfCodePoints) {
			return createSubstrCP().substringCP(codePointStart, nrOfCodePoints);
		}

		private SubstrCP createSubstrCP() {
			return usesFieldRef() ? SubstrCP.valueOf(fieldReference) : SubstrCP.valueOf(expression);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $concat}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Concat extends AbstractAggregationExpression {

		private Concat(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$concat";
		}

		/**
		 * Creates new {@link Concat}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Concat valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Concat(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Concat}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Concat valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Concat(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Concat}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Concat stringValue(String value) {

			Assert.notNull(value, "Value must not be null!");
			return new Concat(Collections.singletonList(value));
		}

		public Concat concatValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Concat(append(Fields.field(fieldReference)));
		}

		public Concat concatValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Concat(append(expression));
		}

		public Concat concat(String value) {
			return new Concat(append(value));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $substr}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Substr extends AbstractAggregationExpression {

		private Substr(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$substr";
		}

		/**
		 * Creates new {@link Substr}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Substr valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Substr(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Substr}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Substr valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Substr(Collections.singletonList(expression));
		}

		public Substr substring(int start) {
			return substring(start, -1);
		}

		public Substr substring(int start, int nrOfChars) {
			return new Substr(append(Arrays.asList(start, nrOfChars)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toLower}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ToLower extends AbstractAggregationExpression {

		private ToLower(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toLower";
		}

		/**
		 * Creates new {@link ToLower}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ToLower lowerValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ToLower(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ToLower}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static ToLower lowerValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ToLower(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ToLower}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ToLower lower(String value) {

			Assert.notNull(value, "Value must not be null!");
			return new ToLower(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toUpper}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ToUpper extends AbstractAggregationExpression {

		private ToUpper(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toUpper";
		}

		/**
		 * Creates new {@link ToUpper}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static ToUpper upperValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ToUpper(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ToUpper}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static ToUpper upperValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ToUpper(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ToUpper}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ToUpper upper(String value) {

			Assert.notNull(value, "Value must not be null!");
			return new ToUpper(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $strcasecmp}.
	 *
	 * @author Christoph Strobl
	 */
	public static class StrCaseCmp extends AbstractAggregationExpression {

		private StrCaseCmp(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$strcasecmp";
		}

		/**
		 * Creates new {@link StrCaseCmp}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StrCaseCmp valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StrCaseCmp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StrCaseCmp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static StrCaseCmp valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StrCaseCmp(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link StrCaseCmp}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static StrCaseCmp stringValue(String value) {

			Assert.notNull(value, "Value must not be null!");
			return new StrCaseCmp(Collections.singletonList(value));
		}

		public StrCaseCmp strcasecmp(String value) {
			return new StrCaseCmp(append(value));
		}

		public StrCaseCmp strcasecmpValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new StrCaseCmp(append(Fields.field(fieldReference)));
		}

		public StrCaseCmp strcasecmpValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StrCaseCmp(append(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $indexOfBytes}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IndexOfBytes extends AbstractAggregationExpression {

		private IndexOfBytes(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$indexOfBytes";
		}

		/**
		 * Start creating a new {@link IndexOfBytes}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static SubstringBuilder valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new SubstringBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating a new {@link IndexOfBytes}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SubstringBuilder valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SubstringBuilder(expression);
		}

		/**
		 * Optionally define the substring search start and end position.
		 *
		 * @param range must not be {@literal null}.
		 * @return
		 */
		public IndexOfBytes within(Range<Long> range) {
			return new IndexOfBytes(append(AggregationUtils.toRangeValues(range)));
		}

		public static class SubstringBuilder {

			private final Object stringExpression;

			private SubstringBuilder(Object stringExpression) {
				this.stringExpression = stringExpression;
			}

			/**
			 * Creates a new {@link IndexOfBytes} given {@literal substring}.
			 *
			 * @param substring must not be {@literal null}.
			 * @return
			 */
			public IndexOfBytes indexOf(String substring) {
				return new IndexOfBytes(Arrays.asList(stringExpression, substring));
			}

			/**
			 * Creates a new {@link IndexOfBytes} given {@link AggregationExpression} that resolves to the substring.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public IndexOfBytes indexOf(AggregationExpression expression) {
				return new IndexOfBytes(Arrays.asList(stringExpression, expression));
			}

			/**
			 * Creates a new {@link IndexOfBytes} given {@link Field} that resolves to the substring.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public IndexOfBytes indexOf(Field fieldReference) {
				return new IndexOfBytes(Arrays.asList(stringExpression, fieldReference));
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $indexOfCP}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IndexOfCP extends AbstractAggregationExpression {

		private IndexOfCP(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$indexOfCP";
		}

		/**
		 * Start creating a new {@link IndexOfCP}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static SubstringBuilder valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new SubstringBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating a new {@link IndexOfCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SubstringBuilder valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SubstringBuilder(expression);
		}

		/**
		 * Optionally define the substring search start and end position.
		 *
		 * @param range must not be {@literal null}.
		 * @return
		 */
		public IndexOfCP within(Range<Long> range) {
			return new IndexOfCP(append(AggregationUtils.toRangeValues(range)));
		}

		public static class SubstringBuilder {

			private final Object stringExpression;

			private SubstringBuilder(Object stringExpression) {
				this.stringExpression = stringExpression;
			}

			/**
			 * Creates a new {@link IndexOfCP} given {@literal substring}.
			 *
			 * @param substring must not be {@literal null}.
			 * @return
			 */
			public IndexOfCP indexOf(String substring) {
				return new IndexOfCP(Arrays.asList(stringExpression, substring));
			}

			/**
			 * Creates a new {@link IndexOfCP} given {@link AggregationExpression} that resolves to the substring.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			public IndexOfCP indexOf(AggregationExpression expression) {
				return new IndexOfCP(Arrays.asList(stringExpression, expression));
			}

			/**
			 * Creates a new {@link IndexOfCP} given {@link Field} that resolves to the substring.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return
			 */
			public IndexOfCP indexOf(Field fieldReference) {
				return new IndexOfCP(Arrays.asList(stringExpression, fieldReference));
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $split}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Split extends AbstractAggregationExpression {

		private Split(List<?> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
			return "$split";
		}

		/**
		 * Start creating a new {@link Split}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Split valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Split(asFields(fieldReference));
		}

		/**
		 * Start creating a new {@link Split}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Split valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Split(Collections.singletonList(expression));
		}

		/**
		 * Use given {@link String} as delimiter.
		 *
		 * @param delimiter must not be {@literal null}.
		 * @return
		 */
		public Split split(String delimiter) {

			Assert.notNull(delimiter, "Delimiter must not be null!");
			return new Split(append(delimiter));
		}

		/**
		 * Use value of referenced field as delimiter.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public Split split(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Split(append(fieldReference));
		}

		/**
		 * Use value resulting from {@link AggregationExpression} as delimiter.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public Split split(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Split(append(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $strLenBytes}.
	 *
	 * @author Christoph Strobl
	 */
	public static class StrLenBytes extends AbstractAggregationExpression {

		private StrLenBytes(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$strLenBytes";
		}

		/**
		 * Creates new {@link StrLenBytes}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StrLenBytes stringLengthOf(String fieldReference) {
			return new StrLenBytes(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link StrLenBytes}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static StrLenBytes stringLengthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StrLenBytes(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $strLenCP}.
	 *
	 * @author Christoph Strobl
	 */
	public static class StrLenCP extends AbstractAggregationExpression {

		private StrLenCP(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$strLenCP";
		}

		/**
		 * Creates new {@link StrLenCP}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static StrLenCP stringLengthOfCP(String fieldReference) {
			return new StrLenCP(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link StrLenCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static StrLenCP stringLengthOfCP(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new StrLenCP(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $substrCP}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SubstrCP extends AbstractAggregationExpression {

		private SubstrCP(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$substrCP";
		}

		/**
		 * Creates new {@link SubstrCP}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static SubstrCP valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new SubstrCP(asFields(fieldReference));
		}

		/**
		 * Creates new {@link SubstrCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SubstrCP valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SubstrCP(Collections.singletonList(expression));
		}

		public SubstrCP substringCP(int start) {
			return substringCP(start, -1);
		}

		public SubstrCP substringCP(int start, int nrOfChars) {
			return new SubstrCP(append(Arrays.asList(start, nrOfChars)));
		}
	}
}
