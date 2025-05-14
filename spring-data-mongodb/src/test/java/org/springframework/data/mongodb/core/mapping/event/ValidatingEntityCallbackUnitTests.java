/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.assertj.core.api.Assertions.*;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.Ordered;

/**
 * Unit tests for {@link ValidatingEntityCallback}.
 *
 * @author Rene Felgenträger
 * @author Mark Paluch
 * @author yangchef1
 */
class ValidatingEntityCallbackUnitTests {

	private ValidatingEntityCallback callback;

	@BeforeEach
	void setUp() {
		try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
			callback = new ValidatingEntityCallback(factory.getValidator());
		}
	}

	@Test // GH-4910
	void validationThrowsException() {

		Coordinates coordinates = new Coordinates(-1, -1);

		assertThatExceptionOfType(ConstraintViolationException.class).isThrownBy(
						() -> callback.onBeforeSave(coordinates, coordinates.toDocument(), "coordinates"))
				.satisfies(e -> assertThat(e.getConstraintViolations()).hasSize(2));
	}

	@Test // GH-4910
	void validateSuccessful() {

		Coordinates coordinates = new Coordinates(0, 0);
		Object entity = callback.onBeforeSave(coordinates, coordinates.toDocument(), "coordinates");

		assertThat(entity).isEqualTo(coordinates);
	}

	@Test // GH-4914
	void allowsChangingOrderDynamically() {

		assertThat(callback).isInstanceOf(Ordered.class);
		assertThat(callback.getOrder()).isEqualTo(100);

		callback.setOrder(50);
		assertThat(callback.getOrder()).isEqualTo(50);
	}

	record Coordinates(@NotNull @Min(0) Integer x, @NotNull @Min(0) Integer y) {

		Document toDocument() {
			return Document.parse("""
					{
					  "x": %d,
					  "y": %d
					}
					""".formatted(x, y));
		}
	}

}
