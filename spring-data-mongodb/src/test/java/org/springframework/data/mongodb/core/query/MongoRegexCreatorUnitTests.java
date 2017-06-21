package org.springframework.data.mongodb.core.query;

import static java.util.Arrays.*;
import static org.springframework.data.mongodb.core.query.MongoRegexCreatorUnitTests.TestParameter.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.StringAssert;
import org.junit.Test;
import org.springframework.data.repository.query.parser.Part.Type;

/**
 * Tests the creation of Regex's in {@link MongoRegexCreator}
 * 
 * @author Jens Schauder
 */
public class MongoRegexCreatorUnitTests {

	List<TestParameter> testParameters = asList(TestParameter.test("anystring", null, "anystring", "type=null -> input"),
			test(null, Type.AFTER, null, "source=null -> null"), //
			test("anystring", Type.REGEX, "anystring", "REGEX -> input"), //
			test("one.two?three", Type.AFTER, "\\Qone.two?three\\E",
					"not(REGEX, LIKE, NOT_LIKE, PunctuationPattern -> quoted punctuation"), //
			test("*", Type.LIKE, ".*", "LIKE * -> .*"), test("*", Type.NOT_LIKE, ".*", "LIKE * -> .*"), //
			test("*.*", Type.LIKE, ".*\\Q.\\E.*", "Wildcards & Punctuation"), //
			test("*.", Type.LIKE, ".*\\Q.\\E", "Leading Wildcard & Punctuation"), //
			test(".*", Type.LIKE, "\\Q.\\E.*", "Trailing Wildcard & Punctuation"), //
			test("other", Type.LIKE, "other", "No Wildcard & Other"), //
			test("other*", Type.LIKE, "other.*", "Trailing Wildcard & Other"), //
			test("*other", Type.LIKE, ".*other", "Leading Wildcard & Other"), //
			test("o*t.*h.er", Type.LIKE, "\\Qo*t.*h.er\\E", "Dots & Stars"), //
			test("other", Type.STARTING_WITH, "^other", "Dots & Stars"), //
			test("other", Type.ENDING_WITH, "other$", "Dots & Stars"), //
			test("other", Type.CONTAINING, ".*other.*", "Dots & Stars"), //
			test("other", Type.NOT_CONTAINING, ".*other.*", "Dots & Stars"), //
			test("other", Type.SIMPLE_PROPERTY, "^other$", "Dots & Stars"), //
			test("other", Type.NEGATING_SIMPLE_PROPERTY, "^other$", "Dots & Stars"));

	Map<Type, String> expectedResultsForAllTypes = new HashMap<>();
	{
		expectedResultsForAllTypes.put(Type.BETWEEN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.IS_NOT_NULL, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.IS_NULL, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.LESS_THAN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.LESS_THAN_EQUAL, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.GREATER_THAN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.GREATER_THAN_EQUAL, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.BEFORE, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.AFTER, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.NOT_LIKE, "\\Qo*t.*h.er\\E.*");
		expectedResultsForAllTypes.put(Type.LIKE, "\\Qo*t.*h.er\\E.*");
		expectedResultsForAllTypes.put(Type.STARTING_WITH, "^\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.ENDING_WITH, "\\Qo*t.*h.er*\\E$");
		expectedResultsForAllTypes.put(Type.IS_NOT_EMPTY, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.IS_EMPTY, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.NOT_CONTAINING, ".*\\Qo*t.*h.er*\\E.*");
		expectedResultsForAllTypes.put(Type.CONTAINING, ".*\\Qo*t.*h.er*\\E.*");
		expectedResultsForAllTypes.put(Type.NOT_IN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.IN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.NEAR, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.WITHIN, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.REGEX, "o*t.*h.er*");
		expectedResultsForAllTypes.put(Type.EXISTS, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.TRUE, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.FALSE, "\\Qo*t.*h.er*\\E");
		expectedResultsForAllTypes.put(Type.NEGATING_SIMPLE_PROPERTY, "^\\Qo*t.*h.er*\\E$");
		expectedResultsForAllTypes.put(Type.SIMPLE_PROPERTY, "^\\Qo*t.*h.er*\\E$");

	}

	@Test
	public void testSpecialCases() {
		SoftAssertions.assertSoftly(sa -> testParameters.forEach(tp -> tp.check(sa)));
	}

	@Test
	public void testAllTypes() {
		SoftAssertions.assertSoftly(
				sa -> Arrays.stream(Type.values()).forEach(t -> //
						test("o*t.*h.er*", t, expectedResultsForAllTypes.getOrDefault(t,"missed one"), t.toString())//
								.check(sa)));
	}

	static class TestParameter {

		TestParameter(String source, Type type, String expectedResult, String comment) {
			this.source = source;
			this.type = type;
			this.expectedResult = expectedResult;
			this.comment = comment;
		}

		static TestParameter test(String source, Type type, String expectedResult, String comment) {
			return new TestParameter(source, type, expectedResult, comment);
		}

		private final String source;
		private final Type type;
		private final String expectedResult;
		private final String comment;

		private StringAssert check(SoftAssertions sa) {

			return sa
					.assertThat( //
							MongoRegexCreator.INSTANCE.toRegularExpression(source, type)) //
					.describedAs(comment) //
					.isEqualTo(expectedResult);
		}
	}

}
