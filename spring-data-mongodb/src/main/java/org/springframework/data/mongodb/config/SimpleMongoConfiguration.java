/**
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.mongodb.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.util.StringUtils;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * Allows for the creation of a {@link MongoTemplate} with custom converters in Java.  If no
 * database is provided, it will write to the database "test" that is default on MongoDB.  If the
 * server address(es) are not provided, it will connect to the localhost on the default port.
 * 
 * @author Chuong Ngo
 */
public class SimpleMongoConfiguration extends AbstractMongoConfiguration {
	protected String databaseName;
	protected List<Converter<?, ?>> converters;
	protected UserCredentials userCredentials;
	protected Mongo mongo;

	/**
	 * Sets the database that the {@link MongoTemplate} will connect to.
	 * 
	 * @param databaseName -	the name of the database to connect to.
	 */
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	@Override
	protected String getDatabaseName() {
		if(!StringUtils.hasText(databaseName)) {
			databaseName = "test";
		}
		
		return databaseName;
	}

	@Override
	public Mongo mongo() throws Exception {
		if(mongo == null) {
			mongo = new MongoClient();
		}
		
		return mongo;
	}
	
	/**
	 * Sets the {@link Mongo} to be used by the {@link MongoTemplate}.  This determines the servers
	 * that will be connected to, the {@link WriteConcern}, etc...
	 * 
	 * @param mongo	-	the {@link Mongo} to be used.
	 */
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}
	
	@Override
	public MongoTemplate mongoTemplate() throws Exception {
		MappingMongoConverter converter = mappingMongoConverter();
		converter.afterPropertiesSet();
		return new MongoTemplate(mongoDbFactory(), converter);
	}
	
	@Override
	public CustomConversions customConversions() {
		return new CustomConversions(getConverters());
	}

	/**
	 * Returns the list of custom converters to be registered with the {@link MongoTemplate}.
	 * 
	 * @return	a list of converters to be registered with the {@link MongoTemplate}.
	 */
	public List<Converter<?, ?>> getConverters() {
		if(converters == null) {
			converters = new ArrayList<Converter<?, ?>>();
		}
		
		return converters;
	}
	
	/**
	 * Adds a custom converter to the list to be registered with the {@link MongoTemplate}.
	 * 
	 * @param converter -	the converter to be added to the list.
	 */
	public void addConverter(Converter<?, ?> converter) {
		if(converters == null) {
			converters = new ArrayList<Converter<?, ?>>();
		}
		
		if(converter != null) {
			converters.add(converter);
		}
	}

	/**
	 * Sets the converters to be registered with the {@link MongoTemplate}
	 * 
	 * @param converters -	the list of converters to be used.
	 */
	public void setConverters(List<Converter<?, ?>> converters) {
		this.converters = converters;
	}

	@Override
	public UserCredentials getUserCredentials() {
		return userCredentials;
	}

	/**
	 * Sets the credentials to be used by {@link Mongo} to connect to the MongoDB servers.
	 * 
	 * @param userCredentials -	the credentials to be used.
	 */
	public void setUserCredentials(UserCredentials userCredentials) {
		this.userCredentials = userCredentials;
	}
}
