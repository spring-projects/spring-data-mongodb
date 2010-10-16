package org.springframework.datastore.document.mongodb.analytics;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.datastore.document.analytics.MvcEvent;
import org.springframework.datastore.document.analytics.Parameters;
import org.springframework.datastore.document.mongodb.MongoTemplate;
import org.springframework.datastore.document.mongodb.query.Query;
//import org.springframework.datastore.document.mongodb.query.QueryBuilder;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;

public class MvcAnalyticsTests {

	private MongoTemplate mongoTemplate;
	
	@Before
	public void setUp() throws Exception {
		Mongo m = new Mongo();
		DB db = m.getDB("mvc");
		mongoTemplate = new MongoTemplate(db);
		mongoTemplate.setDefaultCollectionName("mvc");
		mongoTemplate.afterPropertiesSet();
		//mongoTemplate.dropCollection("mvc");
		//mongoTemplate.createCollection("mvc");
	}
	
	@Test
	public void loadData() {
		
		//datasize, favoriteRestId
		createAndStoreForP1(5, 1);
		createAndStoreForP1(6, 2);
		createAndStoreForP1(3, 3);
		createAndStoreForP1(8, 4);
		
		
		List<MvcEvent> mvcEvents = mongoTemplate.queryForCollection("mvc", MvcEvent.class);
		Assert.assertEquals(22, mvcEvents.size());
		
	}
	
	/*
	 
var start = new Date(2010,9,1);
var end = new Date(2010,11,1);
db.mvc.group(
   { cond: {"action": "addFavoriteRestaurant", "date": {$gte: start, $lt: end}}
   , key: {"parameters.p1": true}
   , initial: {count: 0}
   , reduce: function(doc, out){ out.count++; }
   } );
   
	 */

	@Test
	public void listAll() {
		List<MvcEvent> mvcEvents = mongoTemplate.queryForCollection("mvc", MvcEvent.class);
		for (MvcEvent mvcEvent : mvcEvents) {
			System.out.println(mvcEvent.getDate());
		}
		//System.out.println(mvcEvents);
	}
	
	@Test
	public void groupQuery() {
		//This circumvents exception translation
		DBCollection collection = mongoTemplate.getConnection().getCollection("mvc");
		
		//QueryBuilder qb = new QueryBuilder();
		//qb.start("date").greaterThan(object)
	    Calendar startDate = Calendar.getInstance();
	    startDate.clear();
	    startDate.set(Calendar.YEAR, 2010);
	    startDate.set(Calendar.MONTH, 5);
	    Calendar endDate = Calendar.getInstance();
	    endDate.clear();
	    endDate.set(Calendar.YEAR, 2010);
	    endDate.set(Calendar.MONTH, 12);
	    
	    /*
		QueryBuilder qb = new QueryBuilder();
		Query q = qb.find("date").gte(startDate.getTime()).lt(endDate.getTime()).and("action").is("addFavoriteRestaurant").build();
		DBObject cond2 = q.getQueryObject();
		*/
	    DBObject cond = QueryBuilder.start("date").greaterThanEquals(startDate.getTime()).lessThan(endDate.getTime()).and("action").is("addFavoriteRestaurant").get();	    
		DBObject key = new BasicDBObject("parameters.p1", true);
		/*
		DBObject dateQ = new BasicDBObject();
		dateQ.put("$gte", startDate.getTime());
		dateQ.put("$lt", endDate.getTime());
		DBObject cond = new BasicDBObject();
		cond.put("action", "addFavoriteRestaurant");
		cond.put("date", dateQ);*/
	
		DBObject intitial = new BasicDBObject("count", 0);
		DBObject result = collection.group(key, cond, intitial, "function(doc, out){ out.count++; }");
		if (result instanceof BasicDBList) {
			BasicDBList dbList = (BasicDBList) result;
			for (Iterator iterator = dbList.iterator(); iterator.hasNext();) {
				DBObject dbo = (DBObject) iterator.next();
				System.out.println(dbo);
			}
		}
		Map resultMap = result.toMap();
		System.out.println(result);
	}
	
	private void createAndStoreForP1(int dataSize, int p1) {
		for (int i = 0; i < dataSize; i++) {
			MvcEvent event = generateEvent(p1);
			mongoTemplate.save(event);
		}
	}

	private MvcEvent generateEvent(Integer p1) {
		MvcEvent event = new MvcEvent();
		
		event.setController("RestaurantController");
		event.setAction("addFavoriteRestaurant");
		event.setDate(new Date());
		event.setRemoteUser("mpollack");
		event.setRequestAddress("127.0.0.1");
		event.setRequestUri("/myrestaurants-analytics/restaurants");
		Parameters params = new Parameters();
		params.setP1(p1.toString());
		params.setP2("2");
		event.setParameters(params);
		
		return event;
	}
}

