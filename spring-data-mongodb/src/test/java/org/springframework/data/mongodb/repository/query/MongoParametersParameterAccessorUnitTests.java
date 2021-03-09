/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link MongoParametersParameterAccessor}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class MongoParametersParameterAccessorUnitTests {

	Distance DISTANCE = new Distance(2.5, Metrics.KILOMETERS);
	RepositoryMetadata metadata = new DefaultRepositoryMetadata(PersonRepository.class);
	MongoMappingContext context = new MongoMappingContext();
	ProjectionFactory factory = new SpelAwareProxyProjectionFactory();

	@Test
	public void returnsUnboundedForDistanceIfNoneAvailable() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { new Point(10, 20) });
		assertThat(accessor.getDistanceRange().getUpperBound().isBounded()).isFalse();
	}

	@Test
	public void returnsDistanceIfAvailable() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { new Point(10, 20), DISTANCE });
		assertThat(accessor.getDistanceRange().getUpperBound()).isEqualTo(Bound.inclusive(DISTANCE));
	}

	@Test // DATAMONGO-973
	public void shouldReturnAsFullTextStringWhenNoneDefinedForMethod() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { new Point(10, 20), DISTANCE });
		assertThat(accessor.getFullText()).isNull();
	}

	@Test // DATAMONGO-973
	public void shouldProperlyConvertTextCriteria() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, TextCriteria.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { "spring", TextCriteria.forDefaultLanguage().matching("data") });
		assertThat(accessor.getFullText().getCriteriaObject().toJson())
				.isEqualTo(Document.parse("{ \"$text\" : { \"$search\" : \"data\"}}").toJson());
	}

	@Test // DATAMONGO-1110
	public void shouldDetectMinAndMaxDistance() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Range.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		Distance min = new Distance(10, Metrics.KILOMETERS);
		Distance max = new Distance(20, Metrics.KILOMETERS);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { new Point(10, 20), Distance.between(min, max) });

		Range<Distance> range = accessor.getDistanceRange();

		assertThat(range.getLowerBound()).isEqualTo(Bound.inclusive(min));
		assertThat(range.getUpperBound()).isEqualTo(Bound.inclusive(max));
	}

	@Test // DATAMONGO-1854
	public void shouldDetectCollation() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Collation.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		Collation collation = Collation.of("en_US");
		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { "dalinar", collation });

		assertThat(accessor.getCollation()).isEqualTo(collation);
	}

	@Test // GH-2107
	public void shouldReturnUpdateIfPresent() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findAndModifyByFirstname", String.class, UpdateDefinition.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		Update update = new Update();
		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { "dalinar", update });

		assertThat(accessor.getUpdate()).isSameAs(update);
	}

	@Test // GH-2107
	public void shouldReturnNullIfNoUpdatePresent() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { new Point(0,0) });

		assertThat(accessor.getUpdate()).isNull();
	}

	interface PersonRepository extends Repository<Person, Long> {

		List<Person> findByLocationNear(Point point);

		List<Person> findByLocationNear(Point point, Distance distance);

		List<Person> findByLocationNear(Point point, Range<Distance> distances);

		List<Person> findByFirstname(String firstname, TextCriteria fullText);

		List<Person> findByFirstname(String firstname, Collation collation);

		List<Person> findAndModifyByFirstname(String firstname, UpdateDefinition update);

	}
}
