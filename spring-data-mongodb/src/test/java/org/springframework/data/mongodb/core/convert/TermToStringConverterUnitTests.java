/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.convert.MongoConverters.TermToStringConverter;
import org.springframework.data.mongodb.core.text.Term;
import org.springframework.data.mongodb.core.text.Term.Type;

/**
 * @author Christoph Strobl
 */
public class TermToStringConverterUnitTests {

	/**
	 * @DATAMONGO-973
	 */
	@Test
	public void shouldNotConvertNull() {
		assertThat(TermToStringConverter.INSTANCE.convert(null), nullValue());
	}

	/**
	 * @DATAMONGO-973
	 */
	@Test
	public void shouldUseFormattedRepresentationForConversion() {

		Term term = spy(new Term("foo", Type.WORD));
		TermToStringConverter.INSTANCE.convert(term);
		verify(term, times(1)).getFormatted();
	}
}
