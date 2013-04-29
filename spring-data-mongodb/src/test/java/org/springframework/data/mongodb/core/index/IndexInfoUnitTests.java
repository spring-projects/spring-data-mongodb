/*
 * Copyright 2012-2013 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for {@link IndexInfo}.
 * 
 * @author Oliver Gierke
 */
public class IndexInfoUnitTests {

	@Test
	public void isIndexForFieldsCorrectly() {

		IndexField fooField = IndexField.create("foo", Direction.ASC);
		IndexField barField = IndexField.create("bar", Direction.DESC);

		IndexInfo info = new IndexInfo(Arrays.asList(fooField, barField), "myIndex", false, false, false);
		assertThat(info.isIndexForFields(Arrays.asList("foo", "bar")), is(true));
	}
}
