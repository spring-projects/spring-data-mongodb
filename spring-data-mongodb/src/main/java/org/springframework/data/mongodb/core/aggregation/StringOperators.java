/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.util.RegexFlags;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal String} aggregation operations.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Divya Srivastava
 * @since 1.10
 */
public class StringOperators {

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link StringOperatorFactory}.
	 */
	public static StringOperatorFactory valueOf(String fieldReference) {
		return new StringOperatorFactory(fieldReference);
	}

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link StringOperatorFactory}.
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link StringOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public StringOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats the value
		 * of the referenced field to it.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public Concat concatValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createConcat().concatValueOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats the result
		 * of the given {@link AggregationExpression} to it.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public Concat concatValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createConcat().concatValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and concats given
		 * {@literal value} to it.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public Concat concat(String value) {

			Assert.notNull(value, "Value must not be null");
			return createConcat().concat(value);
		}

		private Concat createConcat() {
			return usesFieldRef() ? Concat.valueOf(fieldReference) : Concat.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified index position.
		 *
		 * @param start start index number (including)
		 * @return new instance of {@link Substr}.
		 */
		public Substr substring(int start) {
			return substring(start, -1);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified index position including the specified number of characters.
		 *
		 * @param start start index number (including)
		 * @param nrOfChars number of characters.
		 * @return new instance of {@link Substr}.
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
		 * @return new instance of {@link ToLower}.
		 */
		public ToLower toLower() {
			return usesFieldRef() ? ToLower.lowerValueOf(fieldReference) : ToLower.lowerValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and uppers it.
		 *
		 * @return new instance of {@link ToUpper}.
		 */
		public ToUpper toUpper() {
			return usesFieldRef() ? ToUpper.upperValueOf(fieldReference) : ToUpper.upperValueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link StrCaseCmp}.
		 */
		public StrCaseCmp strCaseCmp(String value) {

			Assert.notNull(value, "Value must not be null");
			return createStrCaseCmp().strcasecmp(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the referenced {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link StrCaseCmp}.
		 */
		public StrCaseCmp strCaseCmpValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createStrCaseCmp().strcasecmpValueOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and performs
		 * case-insensitive comparison to the result of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StrCaseCmp}.
		 */
		public StrCaseCmp strCaseCmpValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link IndexOfBytes}.
		 */
		public IndexOfBytes indexOf(String substring) {

			Assert.notNull(substring, "Substring must not be null");
			return createIndexOfBytesSubstringBuilder().indexOf(substring);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring contained in the given {@literal field reference} and returns the UTF-8 byte
		 * index (zero-based) of the first occurrence.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IndexOfBytes}.
		 */
		public IndexOfBytes indexOf(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createIndexOfBytesSubstringBuilder().indexOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring resulting from the given {@link AggregationExpression} and returns the UTF-8
		 * byte index (zero-based) of the first occurrence.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IndexOfBytes}.
		 */
		public IndexOfBytes indexOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link IndexOfCP}.
		 */
		public IndexOfCP indexOfCP(String substring) {

			Assert.notNull(substring, "Substring must not be null");
			return createIndexOfCPSubstringBuilder().indexOf(substring);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring contained in the given {@literal field reference} and returns the UTF-8 code
		 * point index (zero-based) of the first occurrence.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IndexOfCP}.
		 */
		public IndexOfCP indexOfCP(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createIndexOfCPSubstringBuilder().indexOf(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and searches a string
		 * for an occurrence of a substring resulting from the given {@link AggregationExpression} and returns the UTF-8
		 * code point index (zero-based) of the first occurrence.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IndexOfCP}.
		 */
		public IndexOfCP indexOfCP(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link Split}.
		 */
		public Split split(String delimiter) {
			return createSplit().split(delimiter);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated string representation into an array of
		 * substrings based on the delimiter resulting from the referenced field..
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Split}.
		 */
		public Split split(Field fieldReference) {
			return createSplit().split(fieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that divides the associated string representation into an array of
		 * substrings based on a delimiter resulting from the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Split}.
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
		 * @return new instance of {@link StrLenBytes}.
		 */
		public StrLenBytes length() {
			return usesFieldRef() ? StrLenBytes.stringLengthOf(fieldReference) : StrLenBytes.stringLengthOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the number of UTF-8 code points in the associated string
		 * representation.
		 *
		 * @return new instance of {@link StrLenCP}.
		 */
		public StrLenCP lengthCP() {
			return usesFieldRef() ? StrLenCP.stringLengthOfCP(fieldReference) : StrLenCP.stringLengthOfCP(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified code point index position.
		 *
		 * @param codePointStart
		 * @return new instance of {@link SubstrCP}.
		 */
		public SubstrCP substringCP(int codePointStart) {
			return substringCP(codePointStart, -1);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and returns a substring
		 * starting at a specified code point index position including the specified number of code points.
		 *
		 * @param codePointStart start point (including).
		 * @param nrOfCodePoints
		 * @return new instance of {@link SubstrCP}.
		 */
		public SubstrCP substringCP(int codePointStart, int nrOfCodePoints) {
			return createSubstrCP().substringCP(codePointStart, nrOfCodePoints);
		}

		private SubstrCP createSubstrCP() {
			return usesFieldRef() ? SubstrCP.valueOf(fieldReference) : SubstrCP.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims whitespaces
		 * from the beginning and end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link Trim}.
		 * @since 2.1
		 */
		public Trim trim() {
			return createTrim();
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the given
		 * character sequence from the beginning and end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 * @since 2.1
		 */
		public Trim trim(String chars) {
			return trim().chars(chars);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the character
		 * sequence resulting from the given {@link AggregationExpression} from the beginning and end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 * @since 2.1
		 */
		public Trim trim(AggregationExpression expression) {
			return trim().charsOf(expression);
		}

		private Trim createTrim() {
			return usesFieldRef() ? Trim.valueOf(fieldReference) : Trim.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims whitespaces
		 * from the beginning. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link LTrim}.
		 * @since 2.1
		 */
		public LTrim ltrim() {
			return createLTrim();
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the given
		 * character sequence from the beginning. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 * @since 2.1
		 */
		public LTrim ltrim(String chars) {
			return ltrim().chars(chars);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the character
		 * sequence resulting from the given {@link AggregationExpression} from the beginning. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 * @since 2.1
		 */
		public LTrim ltrim(AggregationExpression expression) {
			return ltrim().charsOf(expression);
		}

		private LTrim createLTrim() {
			return usesFieldRef() ? LTrim.valueOf(fieldReference) : LTrim.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims whitespaces
		 * from the end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link RTrim}.
		 * @since 2.1
		 */
		public RTrim rtrim() {
			return createRTrim();
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the given
		 * character sequence from the end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 * @since 2.1
		 */
		public RTrim rtrim(String chars) {
			return rtrim().chars(chars);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and trims the character
		 * sequence resulting from the given {@link AggregationExpression} from the end. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 * @since 2.1
		 */
		public RTrim rtrim(AggregationExpression expression) {
			return rtrim().charsOf(expression);
		}

		private RTrim createRTrim() {
			return usesFieldRef() ? RTrim.valueOf(fieldReference) : RTrim.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the given
		 * regular expression to find the document with the first match.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 * @since 3.3
		 */
		public RegexFind regexFind(String regex) {
			return createRegexFind().regex(regex);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression resulting from the given {@link AggregationExpression} to find the document with the first
		 * match.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 * @since 3.3
		 */
		public RegexFind regexFind(AggregationExpression expression) {
			return createRegexFind().regexOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the {@link Pattern} and applies the regular expression with
		 * the options specified in the argument to find the document with the first match.
		 *
		 * @param pattern the pattern object to apply.
		 * @return new instance of {@link RegexFind}.
		 * @since 3.3
		 */
		public RegexFind regexFind(Pattern pattern) {
			return createRegexFind().pattern(pattern);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression with the options specified in the argument to find the document with the first match.
		 *
		 * @param regex the regular expression to apply.
		 * @param options the options to use.
		 * @return new instance of {@link RegexFind}.
		 * @since 3.3
		 */
		public RegexFind regexFind(String regex, String options) {
			return createRegexFind().regex(regex).options(options);
		}

		private RegexFind createRegexFind() {
			return usesFieldRef() ? RegexFind.valueOf(fieldReference) : RegexFind.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the given
		 * regular expression to find all the documents with the match.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 * @since 3.3
		 */
		public RegexFindAll regexFindAll(String regex) {
			return createRegexFindAll().regex(regex);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression resulting from the given {@link AggregationExpression} to find all the documents with the
		 * match..<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 * @since 3.3
		 */
		public RegexFindAll regexFindAll(AggregationExpression expression) {
			return createRegexFindAll().regexOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes a {@link Pattern} and applies the regular expression with
		 * the options specified in the argument to find all the documents with the match.
		 *
		 * @param pattern the pattern object to apply.
		 * @return new instance of {@link RegexFindAll}.
		 * @since 3.3
		 */
		public RegexFindAll regexFindAll(Pattern pattern) {
			return createRegexFindAll().pattern(pattern);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression with the options specified in the argument to find all the documents with the match.
		 *
		 * @param regex the regular expression to apply.
		 * @param options the options to use.
		 * @return new instance of {@link RegexFindAll}.
		 * @since 3.3
		 */
		public RegexFindAll regexFindAll(String regex, String options) {
			return createRegexFindAll().regex(regex).options(options);
		}

		private RegexFindAll createRegexFindAll() {
			return usesFieldRef() ? RegexFindAll.valueOf(fieldReference) : RegexFindAll.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the given
		 * regular expression to find if a match is found or not.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 * @since 3.3
		 */
		public RegexMatch regexMatch(String regex) {
			return createRegexMatch().regex(regex);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression resulting from the given {@link AggregationExpression} to find if a match is found or not.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 * @since 3.3
		 */
		public RegexMatch regexMatch(AggregationExpression expression) {
			return createRegexMatch().regexOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes a {@link Pattern} and applies the regular expression with
		 * the options specified in the argument to find if a match is found or not.
		 *
		 * @param pattern the pattern object to apply.
		 * @return new instance of {@link RegexMatch}.
		 * @since 3.3
		 */
		public RegexMatch regexMatch(Pattern pattern) {
			return createRegexMatch().pattern(pattern);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and applies the regular
		 * expression with the options specified in the argument to find if a match is found or not.
		 *
		 * @param regex the regular expression to apply.
		 * @param options the options to use.
		 * @return new instance of {@link RegexMatch}.
		 * @since 3.3
		 */
		public RegexMatch regexMatch(String regex, String options) {
			return createRegexMatch().regex(regex).options(options);
		}

		private RegexMatch createRegexMatch() {
			return usesFieldRef() ? RegexMatch.valueOf(fieldReference) : RegexMatch.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and replaces the first
		 * occurrence of the search string with the given replacement.
		 * 
		 * @param search
		 * @param replacement
		 * @return new instance of {@link ReplaceOne}.
		 * @since 3.4
		 */
		public ReplaceOne replaceOne(String search, String replacement) {
			return createReplaceOne().find(search).replacement(replacement);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and replaces the first
		 * occurrence of the search string computed by the given {@link AggregationExpression} with the given replacement.
		 *
		 * @param search
		 * @param replacement
		 * @return new instance of {@link ReplaceOne}.
		 * @since 3.4
		 */
		public ReplaceOne replaceOne(AggregationExpression search, String replacement) {
			return createReplaceOne().findValueOf(search).replacement(replacement);
		}

		private ReplaceOne createReplaceOne() {
			return usesFieldRef() ? ReplaceOne.valueOf(fieldReference) : ReplaceOne.valueOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and replaces all
		 * occurrences of the search string with the given replacement.
		 *
		 * @param search
		 * @param replacement
		 * @return new instance of {@link ReplaceOne}.
		 * @since 3.4
		 */
		public ReplaceAll replaceAll(String search, String replacement) {
			return createReplaceAll().find(search).replacement(replacement);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated string representation and replaces all
		 * occurrences of the search string computed by the given {@link AggregationExpression} with the given replacement.
		 *
		 * @param search
		 * @param replacement
		 * @return new instance of {@link ReplaceOne}.
		 * @since 3.4
		 */
		public ReplaceAll replaceAll(AggregationExpression search, String replacement) {
			return createReplaceAll().findValueOf(search).replacement(replacement);
		}

		private ReplaceAll createReplaceAll() {
			return usesFieldRef() ? ReplaceAll.valueOf(fieldReference) : ReplaceAll.valueOf(expression);
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
		 * @return new instance of {@link Concat}.
		 */
		public static Concat valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Concat(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Concat}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public static Concat valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Concat(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Concat}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public static Concat stringValue(String value) {

			Assert.notNull(value, "Value must not be null");
			return new Concat(Collections.singletonList(value));
		}

		/**
		 * Concat the value of the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public Concat concatValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Concat(append(Fields.field(fieldReference)));
		}

		/**
		 * Concat the value resulting from the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
		public Concat concatValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Concat(append(expression));
		}

		/**
		 * Concat the given value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Concat}.
		 */
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
		 * @return new instance of {@link Substr}.
		 */
		public static Substr valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Substr(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Substr}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Substr}.
		 */
		public static Substr valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Substr(Collections.singletonList(expression));
		}

		/**
		 * @param start start index (including)
		 * @return new instance of {@link Substr}.
		 */
		public Substr substring(int start) {
			return substring(start, -1);
		}

		/**
		 * @param start start index (including)
		 * @param nrOfChars
		 * @return new instance of {@link Substr}.
		 */
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ToLower(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ToLower}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static ToLower lowerValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new ToLower(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ToLower}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ToLower lower(String value) {

			Assert.notNull(value, "Value must not be null");
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new ToUpper(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ToUpper}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static ToUpper upperValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new ToUpper(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ToUpper}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static ToUpper upper(String value) {

			Assert.notNull(value, "Value must not be null");
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

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new StrCaseCmp(asFields(fieldReference));
		}

		/**
		 * Creates new {@link StrCaseCmp}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StrCaseCmp}.
		 */
		public static StrCaseCmp valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new StrCaseCmp(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link StrCaseCmp}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link StrCaseCmp}.
		 */
		public static StrCaseCmp stringValue(String value) {

			Assert.notNull(value, "Value must not be null");
			return new StrCaseCmp(Collections.singletonList(value));
		}

		public StrCaseCmp strcasecmp(String value) {
			return new StrCaseCmp(append(value));
		}

		public StrCaseCmp strcasecmpValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new StrCaseCmp(append(Fields.field(fieldReference)));
		}

		public StrCaseCmp strcasecmpValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link SubstringBuilder}.
		 */
		public static SubstringBuilder valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new SubstringBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating a new {@link IndexOfBytes}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link SubstringBuilder}.
		 */
		public static SubstringBuilder valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new SubstringBuilder(expression);
		}

		/**
		 * Optionally define the substring search start and end position.
		 *
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link IndexOfBytes}.
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
			 * @return new instance of {@link IndexOfBytes}.
			 */
			public IndexOfBytes indexOf(String substring) {
				return new IndexOfBytes(Arrays.asList(stringExpression, substring));
			}

			/**
			 * Creates a new {@link IndexOfBytes} given {@link AggregationExpression} that resolves to the substring.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link IndexOfBytes}.
			 */
			public IndexOfBytes indexOf(AggregationExpression expression) {
				return new IndexOfBytes(Arrays.asList(stringExpression, expression));
			}

			/**
			 * Creates a new {@link IndexOfBytes} given {@link Field} that resolves to the substring.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return new instance of {@link IndexOfBytes}.
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
		 * @return new instance of {@link IndexOfCP}.
		 */
		public static SubstringBuilder valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new SubstringBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating a new {@link IndexOfCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IndexOfCP}.
		 */
		public static SubstringBuilder valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new SubstringBuilder(expression);
		}

		/**
		 * Optionally define the substring search start and end position.
		 *
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link IndexOfCP}.
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
			 * @return new instance of {@link IndexOfCP}.
			 */
			public IndexOfCP indexOf(String substring) {
				return new IndexOfCP(Arrays.asList(stringExpression, substring));
			}

			/**
			 * Creates a new {@link IndexOfCP} given {@link AggregationExpression} that resolves to the substring.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link IndexOfCP}.
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
		 * @return new instance of {@link Split}.
		 */
		public static Split valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Split(asFields(fieldReference));
		}

		/**
		 * Start creating a new {@link Split}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Split}.
		 */
		public static Split valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Split(Collections.singletonList(expression));
		}

		/**
		 * Use given {@link String} as delimiter.
		 *
		 * @param delimiter must not be {@literal null}.
		 * @return new instance of {@link Split}.
		 */
		public Split split(String delimiter) {

			Assert.notNull(delimiter, "Delimiter must not be null");
			return new Split(append(delimiter));
		}

		/**
		 * Use value of referenced field as delimiter.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Split}.
		 */
		public Split split(Field fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Split(append(fieldReference));
		}

		/**
		 * Use value resulting from {@link AggregationExpression} as delimiter.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Split}.
		 */
		public Split split(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link StrLenBytes}.
		 */
		public static StrLenBytes stringLengthOf(String fieldReference) {
			return new StrLenBytes(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link StrLenBytes}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StrLenBytes}.
		 */
		public static StrLenBytes stringLengthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link StrLenCP}.
		 */
		public static StrLenCP stringLengthOfCP(String fieldReference) {
			return new StrLenCP(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link StrLenCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link StrLenCP}.
		 */
		public static StrLenCP stringLengthOfCP(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
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
		 * @return new instance of {@link SubstrCP}.
		 */
		public static SubstrCP valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new SubstrCP(asFields(fieldReference));
		}

		/**
		 * Creates new {@link SubstrCP}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link SubstrCP}.
		 */
		public static SubstrCP valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new SubstrCP(Collections.singletonList(expression));
		}

		public SubstrCP substringCP(int start) {
			return substringCP(start, -1);
		}

		public SubstrCP substringCP(int start, int nrOfChars) {
			return new SubstrCP(append(Arrays.asList(start, nrOfChars)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $trim} which removes whitespace or the specified characters from the
	 * beginning and end of a string. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class Trim extends AbstractAggregationExpression {

		private Trim(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Trim} using the value of the provided {@link Field fieldReference} as {@literal input} value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public static Trim valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new Trim(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link Trim} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 */
		public static Trim valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new Trim(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the character(s) to trim from the beginning.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 */
		public Trim chars(String chars) {

			Assert.notNull(chars, "Chars must not be null");
			return new Trim(append("chars", chars));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the character values to trim from the
		 * beginning.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 */
		public Trim charsOf(String fieldReference) {
			return new Trim(append("chars", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the character sequence to trim from the
		 * beginning.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Trim}.
		 */
		public Trim charsOf(AggregationExpression expression) {
			return new Trim(append("chars", expression));
		}

		/**
		 * Remove whitespace or the specified characters from the beginning of a string.<br />
		 *
		 * @return new instance of {@link LTrim}.
		 */
		public LTrim left() {
			return new LTrim(argumentMap());
		}

		/**
		 * Remove whitespace or the specified characters from the end of a string.<br />
		 *
		 * @return new instance of {@link RTrim}.
		 */
		public RTrim right() {
			return new RTrim(argumentMap());
		}

		@Override
		protected String getMongoMethod() {
			return "$trim";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $ltrim} which removes whitespace or the specified characters from the
	 * beginning of a string. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class LTrim extends AbstractAggregationExpression {

		private LTrim(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link LTrim} using the value of the provided {@link Field fieldReference} as {@literal input} value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public static LTrim valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new LTrim(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link LTrim} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public static LTrim valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new LTrim(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the character(s) to trim from the beginning.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public LTrim chars(String chars) {

			Assert.notNull(chars, "Chars must not be null");
			return new LTrim(append("chars", chars));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the character values to trim from the
		 * beginning.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public LTrim charsOf(String fieldReference) {
			return new LTrim(append("chars", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the character sequence to trim from the
		 * beginning.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link LTrim}.
		 */
		public LTrim charsOf(AggregationExpression expression) {
			return new LTrim(append("chars", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$ltrim";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $rtrim} which removes whitespace or the specified characters from the end
	 * of a string. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class RTrim extends AbstractAggregationExpression {

		private RTrim(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link RTrim} using the value of the provided {@link Field fieldReference} as {@literal input} value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 */
		public static RTrim valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new RTrim(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link RTrim} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 */
		public static RTrim valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new RTrim(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the character(s) to trim from the end.
		 *
		 * @param chars must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 */
		public RTrim chars(String chars) {

			Assert.notNull(chars, "Chars must not be null");
			return new RTrim(append("chars", chars));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the character values to trim from the end.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 */
		public RTrim charsOf(String fieldReference) {
			return new RTrim(append("chars", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the character sequence to trim from the end.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RTrim}.
		 */
		public RTrim charsOf(AggregationExpression expression) {
			return new RTrim(append("chars", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$rtrim";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $regexFind} which applies a regular expression (regex) to a string and
	 * returns information on the first matched substring. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class RegexFind extends AbstractAggregationExpression {

		protected RegexFind(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link RegexFind} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public static RegexFind valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new RegexFind(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link RegexFind} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public static RegexFind valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFind(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the options to use with the regular expression.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind options(String options) {

			Assert.notNull(options, "Options must not be null");

			return new RegexFind(append("options", options));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the options values to use with the regular
		 * expression.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind optionsOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new RegexFind(append("options", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the options values to use with the regular
		 * expression.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind optionsOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFind(append("options", expression));
		}

		/**
		 * Specify the regular expression to apply.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind regex(String regex) {

			Assert.notNull(regex, "Regex must not be null");

			return new RegexFind(append("regex", regex));
		}

		/**
		 * Apply a {@link Pattern} into {@code regex} and {@code options} fields.
		 *
		 * @param pattern must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind pattern(Pattern pattern) {

			Assert.notNull(pattern, "Pattern must not be null");

			Map<String, Object> regex = append("regex", pattern.pattern());
			regex.put("options", RegexFlags.toRegexOptions(pattern.flags()));

			return new RegexFind(regex);
		}

		/**
		 * Specify the reference to the {@link Field field} holding the regular expression to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind regexOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null");

			return new RegexFind(append("regex", Fields.field(fieldReference)));
		}

		/**
		 * Specify the {@link AggregationExpression} evaluating to the regular expression to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFind}.
		 */
		public RegexFind regexOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFind(append("regex", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$regexFind";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $regexFindAll} which applies a regular expression (regex) to a string and
	 * returns information on all the matched substrings. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class RegexFindAll extends AbstractAggregationExpression {

		protected RegexFindAll(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link RegexFindAll} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public static RegexFindAll valueOf(String fieldReference) {
			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new RegexFindAll(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link RegexFindAll} using the result of the provided {@link AggregationExpression} as
		 * {@literal input} value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public static RegexFindAll valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFindAll(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the options to use with the regular expression.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll options(String options) {

			Assert.notNull(options, "Options must not be null");

			return new RegexFindAll(append("options", options));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the options values to use with the regular
		 * expression.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll optionsOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null");

			return new RegexFindAll(append("options", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the options values to use with the regular
		 * expression.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll optionsOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFindAll(append("options", expression));
		}

		/**
		 * Apply a {@link Pattern} into {@code regex} and {@code options} fields.
		 *
		 * @param pattern must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll pattern(Pattern pattern) {

			Assert.notNull(pattern, "Pattern must not be null");

			Map<String, Object> regex = append("regex", pattern.pattern());
			regex.put("options", RegexFlags.toRegexOptions(pattern.flags()));

			return new RegexFindAll(regex);
		}

		/**
		 * Specify the regular expression to apply.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll regex(String regex) {

			Assert.notNull(regex, "Regex must not be null");

			return new RegexFindAll(append("regex", regex));
		}

		/**
		 * Specify the reference to the {@link Field field} holding the regular expression to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll regexOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null");

			return new RegexFindAll(append("regex", Fields.field(fieldReference)));
		}

		/**
		 * Specify the {@link AggregationExpression} evaluating to the regular expression to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexFindAll}.
		 */
		public RegexFindAll regexOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexFindAll(append("regex", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$regexFindAll";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $regexMatch} which applies a regular expression (regex) to a string and
	 * returns a boolean that indicates if a match is found or not. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Divya Srivastava
	 * @since 3.3
	 */
	public static class RegexMatch extends AbstractAggregationExpression {

		protected RegexMatch(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link RegexMatch} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public static RegexMatch valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new RegexMatch(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link RegexMatch} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public static RegexMatch valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexMatch(Collections.singletonMap("input", expression));
		}

		/**
		 * Optional specify the options to use with the regular expression.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch options(String options) {

			Assert.notNull(options, "Options must not be null");

			return new RegexMatch(append("options", options));
		}

		/**
		 * Optional specify the reference to the {@link Field field} holding the options values to use with the regular
		 * expression.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch optionsOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new RegexMatch(append("options", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the options values to use with the regular
		 * expression.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch optionsOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexMatch(append("options", expression));
		}

		/**
		 * Apply a {@link Pattern} into {@code regex} and {@code options} fields.
		 *
		 * @param pattern must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch pattern(Pattern pattern) {

			Assert.notNull(pattern, "Pattern must not be null");

			Map<String, Object> regex = append("regex", pattern.pattern());
			regex.put("options", RegexFlags.toRegexOptions(pattern.flags()));

			return new RegexMatch(regex);
		}

		/**
		 * Specify the regular expression to apply.
		 *
		 * @param regex must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch regex(String regex) {

			Assert.notNull(regex, "Regex must not be null");

			return new RegexMatch(append("regex", regex));
		}

		/**
		 * Specify the reference to the {@link Field field} holding the regular expression to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch regexOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new RegexMatch(append("regex", Fields.field(fieldReference)));
		}

		/**
		 * Optional specify the {@link AggregationExpression} evaluating to the regular expression to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RegexMatch}.
		 */
		public RegexMatch regexOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new RegexMatch(append("regex", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$regexMatch";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $replaceOne} which replaces the first instance of a search string in an
	 * input string with a replacement string. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.4 or later.
	 *
	 * @author Divya Srivastava
	 * @author Christoph Strobl
	 * @since 3.4
	 */
	public static class ReplaceOne extends AbstractAggregationExpression {

		protected ReplaceOne(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ReplaceOne} using the given as {@literal input}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public static ReplaceOne value(String value) {

			Assert.notNull(value, "Value must not be null");

			return new ReplaceOne(Collections.singletonMap("input", value));
		}

		/**
		 * Creates new {@link ReplaceOne} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public static ReplaceOne valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new ReplaceOne(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link ReplaceOne} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public static ReplaceOne valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceOne(Collections.singletonMap("input", expression));
		}

		/**
		 * The string to use to replace the first matched instance of {@code find} in input.
		 *
		 * @param replacement must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne replacement(String replacement) {

			Assert.notNull(replacement, "Replacement must not be null");

			return new ReplaceOne(append("replacement", replacement));
		}

		/**
		 * Specifies the reference to the {@link Field field} holding the string to use to replace the first matched
		 * instance of {@code find} in input.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne replacementOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new ReplaceOne(append("replacement", Fields.field(fieldReference)));
		}

		/**
		 * Specifies the {@link AggregationExpression} evaluating to the string to use to replace the first matched instance
		 * of {@code find} in {@code input}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne replacementOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceOne(append("replacement", expression));
		}

		/**
		 * The string to search for within the given input field.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne find(String value) {

			Assert.notNull(value, "Search string must not be null");

			return new ReplaceOne(append("find", value));
		}

		/**
		 * Specify the reference to the {@link Field field} holding the string to search for within the given input field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne findValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null");

			return new ReplaceOne(append("find", fieldReference));
		}

		/**
		 * Specify the {@link AggregationExpression} evaluating to the the string to search for within the given input
		 * field.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public ReplaceOne findValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceOne(append("find", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$replaceOne";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $replaceAll} which replaces all instances of a search string in an input
	 * string with a replacement string. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.4 or later.
	 *
	 * @author Divya Srivastava
	 * @author Christoph Strobl
	 * @since 3.4
	 */
	public static class ReplaceAll extends AbstractAggregationExpression {

		protected ReplaceAll(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ReplaceAll} using the given as {@literal input}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ReplaceOne}.
		 */
		public static ReplaceAll value(String value) {

			Assert.notNull(value, "Value must not be null");

			return new ReplaceAll(Collections.singletonMap("input", value));
		}

		/**
		 * Creates new {@link ReplaceAll} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public static ReplaceAll valueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new ReplaceAll(Collections.singletonMap("input", Fields.field(fieldReference)));
		}

		/**
		 * Creates new {@link ReplaceAll} using the result of the provided {@link AggregationExpression} as {@literal input}
		 * value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public static ReplaceAll valueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceAll(Collections.singletonMap("input", expression));
		}

		/**
		 * The string to use to replace the first matched instance of {@code find} in input.
		 *
		 * @param replacement must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll replacement(String replacement) {

			Assert.notNull(replacement, "Replacement must not be null");

			return new ReplaceAll(append("replacement", replacement));
		}

		/**
		 * Specifies the reference to the {@link Field field} holding the string to use to replace the first matched
		 * instance of {@code find} in input.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll replacementValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new ReplaceAll(append("replacement", Fields.field(fieldReference)));
		}

		/**
		 * Specifies the {@link AggregationExpression} evaluating to the string to use to replace the first matched instance
		 * of {@code find} in input.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll replacementValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceAll(append("replacement", expression));
		}

		/**
		 * The string to search for within the given input field.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll find(String value) {

			Assert.notNull(value, "Search string must not be null");

			return new ReplaceAll(append("find", value));
		}

		/**
		 * Specify the reference to the {@link Field field} holding the string to search for within the given input field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll findValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "fieldReference must not be null");

			return new ReplaceAll(append("find", fieldReference));
		}

		/**
		 * Specify the {@link AggregationExpression} evaluating to the string to search for within the given input field.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReplaceAll}.
		 */
		public ReplaceAll findValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			return new ReplaceAll(append("find", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$replaceAll";
		}
	}

}
