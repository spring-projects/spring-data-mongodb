package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.DBObject;

/**
 * Tests of the {@link AggregationPipeline}.
 * 
 * @author Tobias Trelle
 */
public class AggregationPipelineTests {

	/** Unit under test. */
	private AggregationPipeline pipeline;
	
	@Before public void setUp() {
		pipeline = new AggregationPipeline();
	}
	
	@Test public void limitOperation() {
		// given
		pipeline.limit(42);
		
		// when
		List<DBObject> rawPipeline = pipeline.getOperations();
		
		// then
		assertDBObject("$limit", 42L, rawPipeline);
	}

	@Test public void skipOperation() {
		// given
		pipeline.skip(5);
		
		// when
		List<DBObject> rawPipeline = pipeline.getOperations();
		
		// then
		assertDBObject("$skip", 5L, rawPipeline);
	}

	@Test public void unwindOperation() {
		// given
		pipeline.unwind("field");
		
		// when
		List<DBObject> rawPipeline = pipeline.getOperations();
		
		// then
		assertDBObject("$unwind", "field", rawPipeline);
	}

	@Test public void matchOperation() {
		// given
		Criteria criteria = new Criteria("title").is("Doc 1");
		pipeline.match( criteria );
		
		// when
		List<DBObject> rawPipeline = pipeline.getOperations();
		
		// then
		assertOneDocument(rawPipeline);
		DBObject match = rawPipeline.get(0);
		DBObject criteriaDoc = (DBObject)match.get("$match");
		assertThat( criteriaDoc, notNullValue() );
		assertSingleDBObject( "title" , "Doc 1", criteriaDoc );
	}

	@Test public void sortOperation() {
		// given
		Sort sort = new Sort(new Sort.Order(Direction.ASC, "n"));
		pipeline.sort( sort );
		
		// when
		List<DBObject> rawPipeline = pipeline.getOperations();
		
		// then
		assertOneDocument(rawPipeline);
		DBObject sortDoc = rawPipeline.get(0);
		DBObject orderDoc = (DBObject)sortDoc.get("$sort");
		assertThat( orderDoc, notNullValue() );
		assertSingleDBObject( "n" , 1, orderDoc );
	}
	
	
	private static void assertOneDocument(List<DBObject> result) {
		assertThat( result, notNullValue() );
		assertThat( result.size(), is(1) );		
	}
	
	private static void assertDBObject(String key, Object value, List<DBObject> result) {
		assertOneDocument(result);
		assertSingleDBObject( key, value, result.get(0) );		
	}

	private static void assertSingleDBObject(String key, Object value, DBObject doc) {
		assertThat( doc.get(key), is(value) );		
	}
	
}
