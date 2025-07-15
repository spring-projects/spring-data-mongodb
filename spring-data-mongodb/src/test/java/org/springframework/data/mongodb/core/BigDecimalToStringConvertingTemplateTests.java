/*
 * Copyright 2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplateTests.TypeWithNumbers;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.BigDecimalRepresentation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;

/**
 * Tests for {@link MongoTemplate} using string representation of {@link BigInteger} values.
 *
 * @author Christoph Strobl
 */
public class BigDecimalToStringConvertingTemplateTests {

	public static final String DB_NAME = "mongo-template-tests";

	static @Client MongoClient client;

	@SuppressWarnings("deprecation") MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureConversion(it -> {
			it.customConversions(
					MongoCustomConversions.create(adapter -> adapter.bigDecimal(BigDecimalRepresentation.STRING)));
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
		});

		cfg.configureAuditing(it -> {
			it.auditingHandler(ctx -> {
				return new IsNewAwareAuditingHandler(PersistentEntities.of(ctx));
			});
		});
	});

	@AfterEach
	void cleanUp() {
		template.flush();
	}

	@Test // DATAMONGO-602
	void testUsingAnInQueryWithBigIntegerId() {

		template.remove(new Query(), PersonWithIdPropertyOfTypeBigInteger.class);

		PersonWithIdPropertyOfTypeBigInteger p1 = new PersonWithIdPropertyOfTypeBigInteger();
		p1.setFirstName("Sven");
		p1.setAge(11);
		p1.setId(new BigInteger("2666666666666666665069473312490162649510603601"));
		template.insert(p1);
		PersonWithIdPropertyOfTypeBigInteger p2 = new PersonWithIdPropertyOfTypeBigInteger();
		p2.setFirstName("Mary");
		p2.setAge(21);
		p2.setId(new BigInteger("2666666666666666665069473312490162649510603602"));
		template.insert(p2);
		PersonWithIdPropertyOfTypeBigInteger p3 = new PersonWithIdPropertyOfTypeBigInteger();
		p3.setFirstName("Ann");
		p3.setAge(31);
		p3.setId(new BigInteger("2666666666666666665069473312490162649510603603"));
		template.insert(p3);
		PersonWithIdPropertyOfTypeBigInteger p4 = new PersonWithIdPropertyOfTypeBigInteger();
		p4.setFirstName("John");
		p4.setAge(41);
		p4.setId(new BigInteger("2666666666666666665069473312490162649510603604"));
		template.insert(p4);

		Query q1 = new Query(Criteria.where("age").in(11, 21, 41));
		List<PersonWithIdPropertyOfTypeBigInteger> results1 = template.find(q1, PersonWithIdPropertyOfTypeBigInteger.class);
		Query q2 = new Query(Criteria.where("firstName").in("Ann", "Mary"));
		List<PersonWithIdPropertyOfTypeBigInteger> results2 = template.find(q2, PersonWithIdPropertyOfTypeBigInteger.class);
		Query q3 = new Query(Criteria.where("id").in(new BigInteger("2666666666666666665069473312490162649510603601"),
				new BigInteger("2666666666666666665069473312490162649510603604")));
		List<PersonWithIdPropertyOfTypeBigInteger> results3 = template.find(q3, PersonWithIdPropertyOfTypeBigInteger.class);
		assertThat(results1.size()).isEqualTo(3);
		assertThat(results2.size()).isEqualTo(2);
		assertThat(results3.size()).isEqualTo(2);
	}

	@Test // DATAMONGO-1404
	void updatesBigNumberValueUsingStringComparisonWhenUsingMaxOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();

		// Note that $max operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		// Therefore "80" is considered greater than "700"
		twn.bigIntegerVal = new BigInteger("600");
		twn.bigDeciamVal = new BigDecimal("700.0");

		template.save(twn);

		Update update = new Update()//
				.max("bigIntegerVal", new BigInteger("70")) //
				.max("bigDeciamVal", new BigDecimal("80")) //
		;

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("70"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("80"));
	}

	@Test // DATAMONGO-1404
	void updatesBigNumberValueUsingStringComparisonWhenUsingMinOperator() {

		TypeWithNumbers twn = new TypeWithNumbers();

		// Note that $max operator uses String comparison for BigDecimal/BigInteger comparison according to BSON sort rules.
		// Therefore "80" is considered greater than "700"
		twn.bigIntegerVal = new BigInteger("80");
		twn.bigDeciamVal = new BigDecimal("90.0");

		template.save(twn);

		Update update = new Update()//
				.min("bigIntegerVal", new BigInteger("700")) //
				.min("bigDeciamVal", new BigDecimal("800"));

		template.updateFirst(query(where("id").is(twn.id)), update, TypeWithNumbers.class);

		TypeWithNumbers loaded = template.find(query(where("id").is(twn.id)), TypeWithNumbers.class).get(0);
		assertThat(loaded.bigIntegerVal).isEqualTo(new BigInteger("700"));
		assertThat(loaded.bigDeciamVal).isEqualTo(new BigDecimal("800"));
	}

}
