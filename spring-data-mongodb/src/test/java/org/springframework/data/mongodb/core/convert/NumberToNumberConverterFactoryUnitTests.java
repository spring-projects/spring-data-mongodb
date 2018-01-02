/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.mongodb.core.convert.MongoConverters.NumberToNumberConverterFactory;

/**
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class NumberToNumberConverterFactoryUnitTests {

	public @Parameter(0) Number source;

	public @Parameter(1) Number expected;

	@Parameters
	public static Collection<Number[]> parameters() {

		Number[] longToInt = new Number[] { new Long(10), new Integer(10) };
		Number[] atomicIntToInt = new Number[] { new AtomicInteger(10), new Integer(10) };
		Number[] atomicIntToDouble = new Number[] { new AtomicInteger(10), new Double(10) };
		Number[] atomicLongToInt = new Number[] { new AtomicLong(10), new Integer(10) };
		Number[] atomicLongToLong = new Number[] { new AtomicLong(10), new Long(10) };

		return Arrays.<Number[]> asList(longToInt, atomicIntToInt, atomicIntToDouble, atomicLongToInt, atomicLongToLong);
	}

	@Test // DATAMONGO-1288
	public void convertsToTargetTypeCorrectly() {
		assertThat(NumberToNumberConverterFactory.INSTANCE.getConverter(expected.getClass()).convert(source), is(expected));
	}
}
