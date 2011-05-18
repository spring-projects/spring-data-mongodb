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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.SomeEnumTest.NumberEnum;
import org.springframework.data.document.mongodb.SomeEnumTest.StringEnum;
import org.springframework.data.document.mongodb.convert.SimpleMongoConverter;
import org.springframework.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class SimpleMongoConverterTests {

	static final String SIMPLE_JSON = "{ \"map\" : { \"foo\" : 3 , \"bar\" : 4}, \"number\" : 15 }";
	static final String COMPLEX_JSON = "{ \"map\" : { \"trade\" : { \"orderType\" : \"BUY\" , \"price\" : 90.5 , \"quantity\" : 0 , \"ticker\" : \"VMW\"}}}";

	SimpleMongoConverter converter;
	DBObject object;

	@Before
	public void setUp() {
		converter = new SimpleMongoConverter();
		converter.afterPropertiesSet();
		object = new BasicDBObject();
	}

	@Test
	public void notNestedObject() {
		User user = new User();
		user.setAccountName("My Account");
		user.setUserName("Mark");
		converter.write(user, object);
		assertEquals("My Account", object.get("accountName"));
		assertEquals("Mark", object.get("userName"));

		User u = converter.read(User.class, object);

		assertEquals("My Account", u.getAccountName());
		assertEquals("Mark", u.getUserName());
	}

	@Test
	public void nestedObject() {
		Portfolio p = createPortfolioWithNoTrades();
		converter.write(p, object);

		assertEquals("High Risk Trading Account", object.get("portfolioName"));
		assertTrue(object.containsField("user"));

		Portfolio cp = converter.read(Portfolio.class, object);

		assertEquals("High Risk Trading Account", cp.getPortfolioName());
		assertEquals("Joe Trader", cp.getUser().getUserName());
		assertEquals("ACCT-123", cp.getUser().getAccountName());

	}

	@Test
	public void objectWithMap() {
		Portfolio p = createPortfolioWithPositions();
		converter.write(p, object);

		Portfolio cp = converter.read(Portfolio.class, object);
		assertEquals("High Risk Trading Account", cp.getPortfolioName());
	}

	@Test
	public void objectWithMapContainingNonPrimitiveTypeAsValue() {
		Portfolio p = createPortfolioWithManagers();
		converter.write(p, object);

		Portfolio cp = converter.read(Portfolio.class, object);
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
		converter.write(b, object);

		TradeBatch b2 = converter.read(TradeBatch.class, object);
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
		tb.setTrades(new Trade[] { t2, t1 });
		tb.setTradeList(Arrays.asList(new Trade[] { t1, t2 }));
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

		SomeEnumTest results = converter.read(SomeEnumTest.class, dbo);
		assertNotNull(results);
		assertEquals(test.getId(), results.getId());
		assertEquals(test.getName(), results.getName());
		assertEquals(test.getStringEnum(), results.getStringEnum());
		assertEquals(test.getNumberEnum(), results.getNumberEnum());
	}

	@Test
	public void serializesClassWithFinalObjectIdCorrectly() throws Exception {

		BasicDBObject object = new BasicDBObject();
		Person person = new Person("Oliver");
		converter.write(person, object);

		assertThat(object.get("class"), is(nullValue()));
		assertThat(object.get("_id"), is((Object) person.getId()));
	}

	@Test
	public void discoversGenericsForType() throws Exception {

		Field field = ReflectionUtils.findField(Sample.class, "map");
		assertListOfStringAndLong(converter.getGenericParameters(field.getGenericType()));
	}

	@Test
	public void writesSimpleMapCorrectly() throws Exception {

		Map<String, Long> map = new HashMap<String, Long>();
		map.put("foo", 1L);
		map.put("bar", 2L);

		Sample sample = new Sample();
		sample.setMap(map);
		sample.setNumber(15L);

		converter.write(sample, object);

		assertThat(object.get("number"), is((Object) 15L));

		Object result = object.get("map");
		assertTrue(result instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, Long> mapResult = (Map<String, Long>) result;
		assertThat(mapResult.size(), is(2));
		assertThat(mapResult.get("foo"), is(1L));
		assertThat(mapResult.get("bar"), is(2L));
	}

	@Test
	public void writesComplexMapCorrectly() throws Exception {

		Trade trade = new Trade();
		trade.setOrderType("BUY");
		trade.setTicker("VMW");
		trade.setPrice(90.50d);

		Map<String, Trade> map = new HashMap<String, Trade>();
		map.put("trade", trade);

		converter.write(new Sample2(map), object);
		DBObject tradeDbObject = new BasicDBObject();
		converter.write(trade, tradeDbObject);

		Object result = object.get("map");
		assertTrue(result instanceof Map);

		@SuppressWarnings("unchecked")
		Map<String, DBObject> mapResult = (Map<String, DBObject>) result;
		assertThat(mapResult.size(), is(1));
		assertThat(mapResult.get("trade"), is(tradeDbObject));
	}

	@Test
	public void readsMapWithSetterCorrectly() throws Exception {

		DBObject input = (DBObject) JSON.parse(SIMPLE_JSON);
		Sample result = converter.read(Sample.class, input);
		assertThat(result.getNumber(), is(15L));

		Map<String, Long> map = result.getMap();
		assertThat(map, is(notNullValue()));
		assertThat(map.size(), is(2));
		assertThat(map.get("foo"), is(3L));
		assertThat(map.get("bar"), is(4L));
	}

	@Test
	public void readsMapWithFieldOnlyCorrectly() throws Exception {

		DBObject input = (DBObject) JSON.parse(COMPLEX_JSON);
		Sample2 result = converter.read(Sample2.class, input);

		Map<String, Trade> map = result.getMap();

		Trade trade = new Trade();
		trade.setOrderType("BUY");
		trade.setTicker("VMW");
		trade.setPrice(90.50d);

		assertThat(map.size(), is(1));
		assertThat(map.get("trade").getTicker(), is("VMW"));
		assertThat(map.get("trade").getOrderType(), is("BUY"));
		assertThat(map.get("trade").getPrice(), is(90.50d));
	}

	@Test
	public void supportsBigIntegerAsIdProperty() throws Exception {

		Sample3 sample3 = new Sample3();
		sample3.id = new BigInteger("4d24809660413b687f5d323e", 16);
		converter.write(sample3, object);
		assertThat(object.get("_id"), is(notNullValue()));

		Sample3 result = converter.read(Sample3.class,
				(DBObject) JSON.parse("{\"_id\" : {\"$oid\" : \"4d24809660413b687f5d323e\" }}"));
		assertThat(result.getId().toString(16), is("4d24809660413b687f5d323e"));
	}

	@Test
	public void convertsAddressCorrectly() {

		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		DBObject dbObject = new BasicDBObject();

		converter.write(address, dbObject);

		assertThat(dbObject.get("city").toString(), is("New York"));
		assertThat(dbObject.get("street").toString(), is("Broadway"));

		Address result = converter.read(Address.class, dbObject);
		assertThat(result.city, is("New York"));
		assertThat(result.street, is("Broadway"));
	}

	@Test
	public void convertsJodaTimeTypesCorrectly() {

		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
		converters.add(new LocalDateToDateConverter());
		converters.add(new DateToLocalDateConverter());

		converter.setCustomConverters(converters);

		AnotherPerson person = new AnotherPerson();
		person.birthDate = new LocalDate();

		DBObject dbObject = new BasicDBObject();
		converter.write(person, dbObject);

		assertTrue(dbObject.get("birthDate") instanceof Date);

		AnotherPerson result = converter.read(AnotherPerson.class, dbObject);
		assertThat(result.getBirthDate(), is(notNullValue()));
	}

	private void assertListOfStringAndLong(List<Class<?>> types) {

		assertThat(types.size(), CoreMatchers.is(2));
		assertEquals(String.class, types.get(0));
		assertEquals(Long.class, types.get(1));
	}

	public static class Sample {

		private Map<String, Long> map;
		private Long number;

		public void setMap(Map<String, Long> map) {
			this.map = map;
		}

		public Map<String, Long> getMap() {
			return map;
		}

		public void setNumber(Long number) {
			this.number = number;
		}

		public Long getNumber() {
			return number;
		}
	}

	public static class Sample2 {

		private final Map<String, Trade> map;

		protected Sample2() {
			this.map = null;
		}

		public Sample2(Map<String, Trade> map) {
			this.map = map;
		}

		public Map<String, Trade> getMap() {
			return map;
		}
	}

	private static class Sample3 {

		private BigInteger id;

		public BigInteger getId() {
			return id;
		}
	}

	public static class Address {
		String street;
		String city;

		public String getStreet() {
			return street;
		}

		public String getCity() {
			return city;
		}
	}

	public static class AnotherPerson {
		LocalDate birthDate;

		public LocalDate getBirthDate() {
			return birthDate;
		}
	}

	private class LocalDateToDateConverter implements Converter<LocalDate, Date> {

		public Date convert(LocalDate source) {
			return source.toDateMidnight().toDate();
		}
	}

	private class DateToLocalDateConverter implements Converter<Date, LocalDate> {

		public LocalDate convert(Date source) {
			return new LocalDate(source.getTime());
		}
	}
}
