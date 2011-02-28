package org.springframework.data.document.persistence;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.Mongo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:/META-INF/spring/applicationContext.xml")
public class CrossStoreMongoTests {

	@Autowired
	private Mongo mongo;

	@Test
//	@Transactional
//	@Rollback(false)
	public void testUserConstructor() {
		int age = 33;
		MongoPerson p = new MongoPerson("Thomas", age);
		//Assert.assertEquals(p.getRedisValue().getString("RedisPerson.name"), p.getName());
		Assert.assertEquals(age, p.getAge());
		p.birthday();
		Assert.assertEquals(1 + age, p.getAge());
	}

}
