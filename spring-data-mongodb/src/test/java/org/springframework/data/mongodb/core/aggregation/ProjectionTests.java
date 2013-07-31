package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.mongodb.DBObject;

/**
 * Tests of {@link Projection}.
 * 
 * @author Tobias Trelle
 */
public class ProjectionTests {

	/** Unit under test. */
	private Projection projection;

	@Before
	public void setUp() {
		projection = new Projection();
	}

	@Test
	public void emptyProjection() {
		// when
		DBObject raw = projection.toDBObject();

		// then
		assertThat(raw, notNullValue());
		assertThat(raw.toMap().isEmpty(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldDetectNullIncludesInConstructor() {
		// when
		new Projection((String[]) null);
		// then: throw expected exception
	}

	@Test
	public void includesWithConstructor() {
		// given
		projection = new Projection("a", "b");

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertThat(raw, notNullValue());
		assertThat(raw.toMap().size(), is(3));
		assertThat((Integer) raw.get("_id"), is(0));
		assertThat((Integer) raw.get("a"), is(1));
		assertThat((Integer) raw.get("b"), is(1));
	}

	@Test
	public void include() {
		// given
		projection.include("a");

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertSingleDBObject("a", 1, raw);
	}

	@Test
	public void exclude() {
		// given
		projection.exclude("a");

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertSingleDBObject("a", 0, raw);
	}

	@Test
	public void includeAlias() {
		// given
		projection.include("a").as("b");

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertSingleDBObject("b", "$a", raw);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectAliasWithoutInclude() {
		// when
		projection.as("b");
		// then: throw expected exception
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectDuplicateAlias() {
		// when
		projection.include("a").as("b").as("c");
		// then: throw expected exception
	}

	@Test
	public void plus() {
		// given
		projection.include("a").plus(10);

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertNotNullDBObject(raw);
		DBObject addition = (DBObject)raw.get("a");
		assertNotNullDBObject(addition);	
		@SuppressWarnings("unchecked")
		List<Object> summands = (List<Object>)addition.get("$add");
		assertThat( summands, notNullValue() );
		assertThat( summands.size(), is(2) );
		assertThat( (String)summands.get(0), is("$a") );
		assertThat( (Integer)summands.get(1), is (10) );
	}

	@Test
	public void plusWithAlias() {
		// given
		projection.include("a").plus(10).as("b");

		// when
		DBObject raw = projection.toDBObject();

		// then
		assertNotNullDBObject(raw);
		DBObject addition = (DBObject)raw.get("b");
		assertNotNullDBObject(addition);	
		@SuppressWarnings("unchecked")
		List<Object> summands = (List<Object>)addition.get("$add");
		assertThat( summands, notNullValue() );
		assertThat( summands.size(), is(2) );
		assertThat( (String)summands.get(0), is("$a") );
		assertThat( (Integer)summands.get(1), is (10) );
	}
	
	
	private static void assertSingleDBObject(String key, Object value, DBObject doc) {
		assertNotNullDBObject(doc);
		assertThat(doc.get(key), is(value));
	}

	private static void assertNotNullDBObject(DBObject doc) {
		assertThat(doc, notNullValue());
		assertThat(doc.toMap().size(), is(1));
	}
}
