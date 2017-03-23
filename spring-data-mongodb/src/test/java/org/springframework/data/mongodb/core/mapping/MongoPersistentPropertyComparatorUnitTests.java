package org.springframework.data.mongodb.core.mapping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity.MongoPersistentPropertyComparator;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link MongoPersistentPropertyComparator}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentPropertyComparatorUnitTests {

	@Mock
	MongoPersistentProperty firstName;

	@Mock
	MongoPersistentProperty lastName;

	@Mock
	MongoPersistentProperty ssn;

	@Test
	public void ordersPropertiesCorrectly() {

		when(ssn.getFieldOrder()).thenReturn(10);
		when(firstName.getFieldOrder()).thenReturn(20);
		when(lastName.getFieldOrder()).thenReturn(Integer.MAX_VALUE);

		List<MongoPersistentProperty> properties = Arrays.asList(firstName, lastName, ssn);
		Collections.sort(properties, MongoPersistentPropertyComparator.INSTANCE);

		assertThat(properties.get(0), is(ssn));
		assertThat(properties.get(1), is(firstName));
		assertThat(properties.get(2), is(lastName));
	}
}
