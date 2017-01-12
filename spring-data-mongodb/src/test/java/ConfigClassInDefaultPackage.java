/*
 * Copyright 2014-2017 the original author or authors.
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
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Sample configuration class in default package.
 * 
 * @author Oliver Gierke
 */
@Configuration
public class ConfigClassInDefaultPackage extends AbstractMongoConfiguration {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.config.AbstractMongoConfiguration#getDatabaseName()
	 */
	@Override
	protected String getDatabaseName() {
		return "default";
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.config.AbstractMongoConfiguration#mongo()
	 */
	@Override
	public Mongo mongo() throws Exception {
		return new MongoClient();
	}
}
