/*
 * Copyright 2010-2011 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.document.mongodb.SomeEnumTest.NumberEnum;
import org.springframework.data.document.mongodb.SomeEnumTest.StringEnum;
import org.springframework.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SimpleMongoConverterTests {
	
	SimpleMongoConverter converter;
	
	@Before
	public void setUp() {
		converter = new SimpleMongoConverter();
	}

	@Test
	public void notNestedObject() {
		User user = new User();
		user.setAccountName("My Account");
		user.setUserName("Mark");
		DBObject dbo = new BasicDBObject();
		converter.write(user, dbo);
		assertEquals("My Account", dbo.get("accountName"));
		assertEquals("Mark", dbo.get("userName"));

		User u = (User) converter.read(User.class, dbo);

		assertEquals("My Account", u.getAccountName());
		assertEquals("Mark", u.getUserName());
	}

	@Test
	public void nestedObject() {
		Portfolio p = createPortfolioWithNoTrades();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);
		
		assertEquals("High Risk Trading Account", dbo.get("portfolioName"));
		assertTrue(dbo.containsField("user"));

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		
		assertEquals("High Risk Trading Account", cp.getPortfolioName());
		assertEquals("Joe Trader", cp.getUser().getUserName());
		assertEquals("ACCT-123", cp.getUser().getAccountName());

	}

	@Test
	public void objectWithMap() {
		Portfolio p = createPortfolioWithPositions();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		assertEquals("High Risk Trading Account", cp.getPortfolioName());
	}

	@Test
	public void objectWithMapContainingNonPrimitiveTypeAsValue() {
		Portfolio p = createPortfolioWithManagers();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		assertEquals("High Risk Trading Account", cp.getPortfolioName());
	}

	protected Portfolio createPortfolioWithPositions() {

		Portfolio portfolio = new Portfolio();
		portfolio.setPortfolioName("High Risk Trading Account");
		Map<String, Integer> positions = new HashMap<String, Integer>();
		positions.put("CSCO", 1);
		portfolio.setPositions(positions);
		return portfolio;
	}

	protected Portfolio createPortfolioWithManagers() {

		Portfolio portfolio = new Portfolio();
		portfolio.setPortfolioName("High Risk Trading Account");
		Map<String, Person> managers = new HashMap<String, Person>();
		Person p1 = new Person();
		p1.setFirstName("Mark");
		managers.put("CSCO", p1);
		portfolio.setPortfolioManagers(managers);
		return portfolio;
	}

	protected Portfolio createPortfolioWithNoTrades() {
		Portfolio portfolio = new Portfolio();
		User user = new User();
		user.setUserName("Joe Trader");
		user.setAccountName("ACCT-123");
		portfolio.setUser(user);
		portfolio.setPortfolioName("High Risk Trading Account");
		return portfolio;
	}

	@Test
	public void objectWithArrayContainingNonPrimitiveType() {
		TradeBatch b = createTradeBatch();
		DBObject dbo = new BasicDBObject();
		converter.write(b, dbo);

		TradeBatch b2 = (TradeBatch) converter.read(TradeBatch.class, dbo);
		assertEquals(b.getBatchId(), b2.getBatchId());
		assertNotNull(b2.getTradeList());
		assertEquals(b.getTradeList().size(), b2.getTradeList().size());
		assertEquals(b.getTradeList().get(1).getTicker(), b2.getTradeList().get(1).getTicker());
		assertEquals(b.getTrades().length, b2.getTrades().length);
		assertEquals(b.getTrades()[1].getTicker(), b2.getTrades()[1].getTicker());
	}

	private TradeBatch createTradeBatch() {
		TradeBatch tb = new TradeBatch();
		tb.setBatchId("123456");
		Trade t1 = new Trade();
		t1.setOrderType("BUY");
		t1.setTicker("AAPL");
		t1.setQuantity(1000);
		t1.setPrice(320.77D);
		Trade t2 = new Trade();
		t2.setOrderType("SELL");
		t2.setTicker("MSFT");
		t2.setQuantity(100);
		t2.setPrice(27.92D);
		tb.setTrades(new Trade[] {t2, t1});
		tb.setTradeList(Arrays.asList(new Trade[] {t1, t2}));
		return tb;
	}

	@Test
	public void objectWithEnumTypes() {
		SomeEnumTest test = new SomeEnumTest();
		test.setId("123AAA");
		test.setName("Sven");
		test.setStringEnum(StringEnum.ONE);
		test.setNumberEnum(NumberEnum.FIVE);
		DBObject dbo = new BasicDBObject();
		converter.write(test, dbo);
		
		SomeEnumTest results = (SomeEnumTest) converter.read(SomeEnumTest.class, dbo);
		assertNotNull(results);
		assertEquals(test.getId(), results.getId());
		assertEquals(test.getName(), results.getName());
		assertEquals(test.getStringEnum(), results.getStringEnum());
		assertEquals(test.getNumberEnum(), results.getNumberEnum());
}

	@Test
	public void testReflection() {
		Method method = ReflectionUtils.findMethod(Portfolio.class, "setPortfolioManagers", Map.class);
		assertNotNull(method);
		List<Class<?>> paramClass = converter.getGenericParameterClass(method);
		
		assertThat(paramClass.isEmpty(), is(false));
		assertThat(paramClass.size(), is(2));
		assertEquals(String.class, paramClass.get(0));
		assertEquals(Person.class, paramClass.get(1));
	}
}
