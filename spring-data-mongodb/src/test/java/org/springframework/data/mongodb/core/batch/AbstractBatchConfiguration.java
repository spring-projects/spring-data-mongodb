package org.springframework.data.mongodb.core.batch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class AbstractBatchConfiguration {
	
	@Configuration
	static class Config extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}
		
		@Bean
		public BatchInsertOperations batchInsertOperation(){
			return new BatchInsert(1000) {};
		}
	}
	
	@Autowired MongoOperations operations;
	@Autowired BatchInsertOperations batch;

	@Before
	@After
	public void cleanUp() {
		
		batch.clear();
		
		for (String collectionName : operations.getCollectionNames()) {
			if (!collectionName.startsWith("system")) {
				operations.execute(collectionName, new CollectionCallback<Void>() {
					@Override
					public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
						collection.remove(new BasicDBObject());
						assertThat(collection.find().hasNext(), is(false));
						return null;
					}
				});
			}
		}
	}
	
	protected List<Person> populateCollection(int quantity){
		
		List<Person> people = new ArrayList<Person>();
		
		for(int index = 0; index < quantity; index++)
			people.add(new Person("Joao"));
		
		return people;
	}
}
