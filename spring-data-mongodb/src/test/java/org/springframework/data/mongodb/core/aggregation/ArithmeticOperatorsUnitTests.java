/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ArithmeticOperators}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mushtaq Ahmed
 * @author Divya Srivastava
 */
class ArithmeticOperatorsUnitTests {

	@Test // DATAMONGO-2370
	void roundShouldWithoutPlace() {

		assertThat(valueOf("field").round().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Collections.singletonList("$field")));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlace() {

		assertThat(valueOf("field").roundToPlace(3).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Arrays.asList("$field", 3)));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlaceFromField() {

		assertThat(valueOf("field").round().placeOf("my-field").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(new Document("$round", Arrays.asList("$field", "$my-field")));
	}

	@Test // DATAMONGO-2370
	void roundShouldWithPlaceFromExpression() {

		assertThat(valueOf("field").round().placeOf((ctx -> new Document("$first", "$source")))
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(new Document("$round", Arrays.asList("$field", new Document("$first", "$source"))));
	}

	@Test // GH-3716
	void rendersDerivativeCorrectly() {

		assertThat(
				valueOf("miles").derivative(SetWindowFieldsOperation.WindowUnits.HOUR).toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $derivative: { input: \"$miles\", unit: \"hour\" } }");
	}

	@Test // GH-3721
	void rendersIntegral() {
		assertThat(valueOf("kilowatts").integral().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $integral : { input : \"$kilowatts\" } }");
	}

	@Test // GH-3721
	void rendersIntegralWithUnit() {
		assertThat(valueOf("kilowatts").integral(SetWindowFieldsOperation.WindowUnits.HOUR)
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $integral : { input : \"$kilowatts\", unit : \"hour\" } }");
	}

	@Test // GH-3728
	void rendersSin() {

		assertThat(valueOf("angle").sin().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $sin : \"$angle\" }");
	}

	@Test // GH-3728
	void rendersSinWithValueInDegrees() {

		assertThat(valueOf("angle").sin(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $sin : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3728
	void rendersSinh() {

		assertThat(valueOf("angle").sinh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $sinh : \"$angle\" }");
	}

	@Test // GH-3728
	void rendersSinhWithValueInDegrees() {

		assertThat(valueOf("angle").sinh(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $sinh : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3708
	void rendersASin() {
		assertThat(valueOf("field").asin().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $asin : \"$field\" }");
	}

	@Test // GH-3708
	void rendersASinh() {
		assertThat(valueOf("field").asinh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $asinh : \"$field\" }");
	}

	@Test // GH-3710
	void rendersCos() {

		assertThat(valueOf("angle").cos().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $cos : \"$angle\" }");
	}

	@Test // GH-3710
	void rendersCosWithValueInDegrees() {

		assertThat(valueOf("angle").cos(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $cos : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3710
	void rendersCosh() {

		assertThat(valueOf("angle").cosh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $cosh : \"$angle\" }");
	}
	
	@Test // GH-3707
	void rendersACos() {
		assertThat(valueOf("field").acos().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $acos : \"$field\" }");
	}

	@Test // GH-3707
	void rendersACosh() {
		assertThat(valueOf("field").acosh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $acosh : \"$field\" }");
	}

	@Test // GH-3710
	void rendersCoshWithValueInDegrees() {

		assertThat(valueOf("angle").cosh(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $cosh : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3730
	void rendersTan() {

		assertThat(valueOf("angle").tan().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $tan : \"$angle\" }");
	}

	@Test // GH-3730
	void rendersTanWithValueInDegrees() {

		assertThat(valueOf("angle").tan(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $tan : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3730
	void rendersTanh() {

		assertThat(valueOf("angle").tanh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $tanh : \"$angle\" }");
	}

	@Test // GH-3730
	void rendersTanhWithValueInDegrees() {

		assertThat(valueOf("angle").tanh(AngularUnit.DEGREES).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $tanh : { $degreesToRadians : \"$angle\" } }");
	}

	@Test // GH-3709
	void rendersATan() {

		assertThat(valueOf("field").atan().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $atan : \"$field\" }");
	}

	@Test // GH-3709
	void rendersATan2() {

		assertThat(valueOf("field1").atan2("field2").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $atan2 : [ \"$field1\" , \"$field2\" ] }");
	}

	@Test // GH-3709
	void rendersATanh() {

		assertThat(valueOf("field").atanh().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $atanh : \"$field\" }");
	}

	@Test // GH-3724
	void rendersRand() {
		assertThat(rand().toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(new Document("$rand", new Document()));
	}
}
