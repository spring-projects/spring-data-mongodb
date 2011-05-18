/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.data.document.mongodb;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class PersonExample {

	private static final Log log = LogFactory.getLog(PersonExample.class);
	@Autowired
	private MongoOperations mongoOps;

	public static void main(String[] args) {
		ApplicationContext applicationContext = new AnnotationConfigApplicationContext(PersonExampleAppConfig.class);
		PersonExample example = applicationContext.getBean(PersonExample.class);
		example.doWork();
	}

	public void doWork() {
		mongoOps.dropCollection("personexample");

		PersonWithIdPropertyOfTypeString p = new PersonWithIdPropertyOfTypeString();
		p.setFirstName("Sven");
		p.setAge(22);

		mongoOps.save(p);

		PersonWithIdPropertyOfTypeString p2 = new PersonWithIdPropertyOfTypeString();
		p2.setFirstName("Jon");
		p2.setAge(23);

		mongoOps.save(p2);

		log.debug("Saved: " + p);

		p = mongoOps.findById(p.getId(), PersonWithIdPropertyOfTypeString.class);

		log.debug("Found: " + p);

		// mongoOps.updateFirst(new Query(where("firstName").is("Sven")), new Update().set("age", 24));

		// mongoOps.updateFirst(new Query(where("firstName").is("Sven")), update("age", 24));

		p = mongoOps.findById(p.getId(), PersonWithIdPropertyOfTypeString.class);
		log.debug("Updated: " + p);

		List<PersonWithIdPropertyOfTypeString> folks = mongoOps.getCollection(PersonWithIdPropertyOfTypeString.class);
		log.debug("Querying for all people...");
		for (PersonWithIdPropertyOfTypeString element : folks) {
			log.debug(element);
		}

		// mongoOps.remove( query(whereId().is(p.getId())), p.getClass());

		mongoOps.remove(p);

		List<PersonWithIdPropertyOfTypeString> people = mongoOps.getCollection(PersonWithIdPropertyOfTypeString.class);

		// PersonWithIdPropertyOfTypeString p2 = mongoOps.findOne(query(whereId().is(p.getId())),
		// PersonWithIdPropertyOfTypeString.class);

		log.debug("Number of people = : " + people.size());

	}

	public void doWork2() {
		mongoOps.dropCollection("personexample");

		PersonWithIdPropertyOfTypeString p = new PersonWithIdPropertyOfTypeString();
		p.setFirstName("Sven");
		p.setAge(22);

	}

}
