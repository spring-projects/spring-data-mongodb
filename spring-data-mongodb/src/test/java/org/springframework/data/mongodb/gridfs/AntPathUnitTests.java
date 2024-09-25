/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.assertj.core.api.Assertions.*;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AntPath}.
 *
 * @author Oliver Gierke
 */
public class AntPathUnitTests {

	@Test
	public void buildRegexCorrectly() {

		AntPath path = new AntPath("**/foo/*-bar.xml");
		String regex = path.toRegex();

		assertThat(Pattern.matches(regex, "foo/bar/foo/foo-bar.xml")).isTrue();
		assertThat(Pattern.matches(regex, "foo/bar/foo/bar/foo-bar.xml")).isFalse();
		assertThat(regex).isEqualTo(".*\\Q/foo/\\E[^/]*\\Q-bar.xml\\E");
	}
}
