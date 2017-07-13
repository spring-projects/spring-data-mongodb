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
package org.springframework.data.mongodb.core.query;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.MongoRegexCreatorUnitTests.TestParameter.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.mongodb.core.query.MongoRegexCreator.MatchMode;

/**
 * Tests the creation of Regex's in {@link MongoRegexCreator}
 * 
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@RunWith(Parameterized.class)
public class MongoRegexCreatorUnitTests {

	@Parameters(name = "{index}: {0}") //
	public static List<TestParameter> parameters() {

		return asList(//
				test(null, MatchMode.EXACT, null, "Null input string -> null"), //
				test("anystring", null, "anystring", "type=null -> input"), //
				test("anystring", MatchMode.REGEX, "anystring", "REGEX -> input"), //
				test("*", MatchMode.LIKE, ".*", "LIKE * -> .*"), //
				test("*.*", MatchMode.LIKE, ".*\\Q.\\E.*", "Wildcards & Punctuation"), //
				test("*.", MatchMode.LIKE, ".*\\Q.\\E", "Leading Wildcard & Punctuation"), //
				test(".*", MatchMode.LIKE, "\\Q.\\E.*", "Trailing Wildcard & Punctuation"), //
				test("other", MatchMode.LIKE, "other", "No Wildcard & Other"), //
				test("other*", MatchMode.LIKE, "other.*", "Trailing Wildcard & Other"), //
				test("*other", MatchMode.LIKE, ".*other", "Leading Wildcard & Other"), //
				test("o*t.*h.er", MatchMode.LIKE, "\\Qo*t.*h.er\\E", "Dots & Stars"), //
				test("other", MatchMode.STARTING_WITH, "^other", "Dots & Stars"), //
				test("other", MatchMode.ENDING_WITH, "other$", "Dots & Stars"), //
				test("other", MatchMode.CONTAINING, ".*other.*", "Dots & Stars"), //
				test("other", MatchMode.EXACT, "^other$", "Dots & Stars"));
	}

	@Parameter(0) //
	public TestParameter parameter;

	@Test
	public void testSpecialCases() {
		parameter.check();
	}

	@lombok.RequiredArgsConstructor(staticName = "test")
	static class TestParameter {

		private final String source;
		private final MatchMode mode;
		private final String expectedResult, comment;

		void check() {

			assertThat(MongoRegexCreator.INSTANCE.toRegularExpression(source, mode))//
					.as(comment)//
					.isEqualTo(expectedResult);
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("Mode: %s, %s", mode, comment);
		}
	}
}
