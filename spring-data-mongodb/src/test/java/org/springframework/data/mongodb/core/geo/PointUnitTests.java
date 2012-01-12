package org.springframework.data.mongodb.core.geo;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.geo.Point;

/**
 * Unit tests for {@link Point}.
 * 
 * @author Oliver Gierke
 */
public class PointUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullforCopyConstructor() {
		new Point(null);
	}

	@Test
	public void equalsIsImplementedCorrectly() {
		assertThat(new Point(1.5, 1.5), is(equalTo(new Point(1.5, 1.5))));
		assertThat(new Point(1.5, 1.5), is(not(equalTo(new Point(2.0, 2.0)))));
		assertThat(new Point(2.0, 2.0), is(not(equalTo(new Point(1.5, 1.5)))));
	}

	@Test
	public void invokingToStringWorksCorrectly() {
		new Point(1.5, 1.5).toString();
	}
}
