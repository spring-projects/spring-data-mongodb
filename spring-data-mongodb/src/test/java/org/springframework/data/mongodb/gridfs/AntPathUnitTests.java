/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.junit.Test;

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

		assertThat(Pattern.matches(regex, "foo/bar/foo/foo-bar.xml"), is(true));
		assertThat(Pattern.matches(regex, "foo/bar/foo/bar/foo-bar.xml"), is(false));
		assertThat(regex, is(".*\\Q/foo/\\E[^/]*\\Q-bar.xml\\E"));
	}
}
