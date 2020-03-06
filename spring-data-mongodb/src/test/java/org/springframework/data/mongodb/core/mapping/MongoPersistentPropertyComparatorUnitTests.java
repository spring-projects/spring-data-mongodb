package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity.MongoPersistentPropertyComparator;

/**
 * Unit tests for {@link MongoPersistentPropertyComparator}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
class MongoPersistentPropertyComparatorUnitTests {

	@Mock MongoPersistentProperty firstName;

	@Mock MongoPersistentProperty lastName;

	@Mock MongoPersistentProperty ssn;

	@Test
	void ordersPropertiesCorrectly() {

		when(ssn.getFieldOrder()).thenReturn(10);
		when(firstName.getFieldOrder()).thenReturn(20);
		when(lastName.getFieldOrder()).thenReturn(Integer.MAX_VALUE);

		List<MongoPersistentProperty> properties = Arrays.asList(firstName, lastName, ssn);
		Collections.sort(properties, MongoPersistentPropertyComparator.INSTANCE);

		assertThat(properties.get(0)).isEqualTo(ssn);
		assertThat(properties.get(1)).isEqualTo(firstName);
		assertThat(properties.get(2)).isEqualTo(lastName);
	}
}
