/*
 * Copyright 2011-2023 the original author or authors.
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
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.User;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Contact;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link MongoQueryMethod}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jorge Rodríguez
 */
public class MongoQueryMethodUnitTests {

	MongoMappingContext context;

	@BeforeEach
	public void setUp() {
		context = new MongoMappingContext();
	}

	@Test
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		MongoEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Address.class);
		assertThat(metadata.getCollectionName()).isEqualTo("contact");
	}

	@Test
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository2.class, "method");
		MongoEntityMetadata<?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType()).isAssignableFrom(Person.class);
		assertThat(entityInformation.getCollectionName()).isEqualTo("person");
	}

	@Test
	public void discoversUserAsDomainTypeForGeoPageQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(PersonRepository.class, "findByLocationNear", Point.class,
				Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery()).isTrue();
		assertThat(queryMethod.isPageQuery()).isTrue();

		queryMethod = queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery()).isTrue();
		assertThat(queryMethod.isPageQuery()).isFalse();
		assertThat(queryMethod.getEntityInformation().getJavaType()).isAssignableFrom(User.class);

		assertThat(queryMethod(PersonRepository.class, "findByEmailAddress", String.class, Point.class).isGeoNearQuery())
				.isTrue();
		assertThat(queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class).isGeoNearQuery())
				.isTrue();
		assertThat(queryMethod(PersonRepository.class, "findByLastname", String.class, Point.class).isGeoNearQuery())
				.isTrue();
	}

	@Test
	public void rejectsGeoPageQueryWithoutPageable() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findByLocationNear", Point.class, Distance.class));
	}

	@Test
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new MongoQueryMethod(method,
				new DefaultRepositoryMetadata(PersonRepository.class), new SpelAwareProxyProjectionFactory(), null));
	}

	@Test
	public void considersMethodReturningGeoPageAsPagingMethod() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findByLocationNear", Point.class, Distance.class,
				Pageable.class);

		assertThat(method.isPageQuery()).isTrue();
		assertThat(method.isCollectionQuery()).isFalse();
	}

	@Test
	public void createsMongoQueryMethodObjectForMethodReturningAnInterface() throws Exception {

		queryMethod(SampleRepository2.class, "methodReturningAnInterface");
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithEmptyMetaCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "emptyMetaAnnotation");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().hasValues()).isFalse();
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec()).isEqualTo(100L);
	}

	@Test // DATAMONGO-1311
	public void createsMongoQueryMethodWithBatchSizeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "batchSize");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getCursorBatchSize()).isEqualTo(100);
	}

	@Test // DATAMONGO-1311
	public void createsMongoQueryMethodWithNegativeBatchSizeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "negativeBatchSize");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getCursorBatchSize()).isEqualTo(-200);
	}

	@Test // DATAMONGO-1403
	public void createsMongoQueryMethodWithSpellFixedMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithSpellFixedMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec()).isEqualTo(100L);
	}

	@Test // DATAMONGO-957
	public void createsMongoQueryMethodWithCommentCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithComment");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getComment()).isEqualTo("foo bar");
	}

	@Test // DATAMONGO-1480
	public void createsMongoQueryMethodWithNoCursorTimeoutCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithNoCursorTimeout");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getFlags())
				.contains(org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT);
	}

	@Test // DATAMONGO-1480, DATAMONGO-2572
	public void createsMongoQueryMethodWithMultipleFlagsCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMultipleFlags");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getFlags()).contains(
				org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT,
				org.springframework.data.mongodb.core.query.Meta.CursorOption.SECONDARY_READS);
	}

	@Test // DATAMONGO-1266
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(User.class);
	}

	@Test // DATAMONGO-2153
	public void findsAnnotatedAggregation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregation");

		assertThat(method.hasAnnotatedAggregation()).isTrue();
		assertThat(method.getAnnotatedAggregation()).hasSize(1);
	}

	@Test // DATAMONGO-2153
	public void detectsCollationForAggregation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregationWithCollation");

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("de_AT");
	}

	@Test // GH-2107
	void detectsModifyingQueryByUpdateType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findAndUpdateBy", String.class, Update.class);

		assertThat(method.isModifyingQuery()).isTrue();
	}

	@Test // GH-2107
	void detectsModifyingQueryByUpdateDefinitionType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findAndUpdateBy", String.class,
				UpdateDefinition.class);

		assertThat(method.isModifyingQuery()).isTrue();
	}

	@Test // GH-2107
	void detectsModifyingQueryByAggregationUpdateDefinitionType() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findAndUpdateBy", String.class,
				AggregationUpdate.class);

		assertThat(method.isModifyingQuery()).isTrue();
	}

	@Test // GH-2107
	void queryCreationFailsOnInvalidUpdate() throws Exception {

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> queryMethod(InvalidUpdateMethodRepo.class, "findAndUpdateByLastname", String.class).verify()) //
				.withMessageContaining("Update") //
				.withMessageContaining("findAndUpdateByLastname");
	}

	@Test // GH-2107
	void queryCreationForUpdateMethodFailsOnInvalidReturnType() throws Exception {

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> queryMethod(InvalidUpdateMethodRepo.class, "findAndIncrementVisitsByFirstname", String.class)
						.verify()) //
				.withMessageContaining("Update") //
				.withMessageContaining("numeric") //
				.withMessageContaining("findAndIncrementVisitsByFirstname");
	}

	@Test // GH-3002
	void readsCollationFromAtCollationAnnotation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findWithCollationFromAtCollationByFirstname",
				String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("en_US");
	}

	@Test // GH-3002
	void readsCollationFromAtQueryAnnotation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findWithCollationFromAtQueryByFirstname",
				String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("en_US");
	}

	@Test // GH-3002
	void annotatedCollationClashSelectsAtCollationAnnotationValue() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class,
				"findWithMultipleCollationsFromAtQueryAndAtCollationByFirstname", String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("de_AT");
	}

	@Test // GH-2971
	void readsReadPreferenceAtQueryAnnotation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findWithReadPreferenceFromAtReadPreferenceByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void readsReadPreferenceFromAtQueryAnnotation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findWithReadPreferenceFromAtQueryByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void annotatedReadPreferenceClashSelectsAtReadPreferenceAnnotationValue() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findWithMultipleReadPreferencesFromAtQueryAndAtReadPreferenceByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void readsReadPreferenceAtRepositoryAnnotation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("primaryPreferred");
	}

	@Test // GH-2971
	void detectsReadPreferenceForAggregation() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregationWithReadPreference");

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-4546
	void errorsOnInvalidAggregation() {

		assertThatIllegalStateException() //
				.isThrownBy(() -> queryMethod(InvalidAggregationMethodRepo.class, "findByAggregation").verify()) //
				.withMessageContaining("Invalid aggregation") //
				.withMessageContaining("findByAggregation");
	}

	private MongoQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new MongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	@ReadPreference(value = "primaryPreferred")
	interface PersonRepository extends Repository<User, Long> {

		// Misses Pageable
		GeoPage<User> findByLocationNear(Point point, Distance distance);

		GeoPage<User> findByLocationNear(Point point, Distance distance, Pageable pageable);

		GeoResult<User> findByEmailAddress(String lastname, Point location);

		GeoResults<User> findByFirstname(String firstname, Point location);

		Collection<GeoResult<User>> findByLastname(String lastname, Point location);

		@Meta
		List<User> emptyMetaAnnotation();

		@Meta(cursorBatchSize = 100)
		List<User> batchSize();

		@Meta(cursorBatchSize = -200)
		List<User> negativeBatchSize();

		@Meta(maxExecutionTimeMs = 100)
		List<User> metaWithMaxExecutionTime();

		@Meta(maxExecutionTimeMs = 100)
		List<User> metaWithSpellFixedMaxExecutionTime();

		@Meta(comment = "foo bar")
		List<User> metaWithComment();

		@Meta(flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT })
		List<User> metaWithNoCursorTimeout();

		@Meta(flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT,
				org.springframework.data.mongodb.core.query.Meta.CursorOption.SECONDARY_READS })
		List<User> metaWithMultipleFlags();

		// DATAMONGO-1266
		void deleteByUserName(String userName);

		@Aggregation("{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }")
		List<User> findByAggregation();

		@Aggregation(pipeline = "{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }",
				collation = "de_AT")
		List<User> findByAggregationWithCollation();

		@Aggregation(pipeline = "{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }", readPreference = "secondaryPreferred")
		List<User> findByAggregationWithReadPreference();

		void findAndUpdateBy(String firstname, Update update);

		void findAndUpdateBy(String firstname, UpdateDefinition update);

		void findAndUpdateBy(String firstname, AggregationUpdate update);

		@Collation("en_US")
		List<User> findWithCollationFromAtCollationByFirstname(String firstname);

		@Query(collation = "en_US")
		List<User> findWithCollationFromAtQueryByFirstname(String firstname);

		@Collation("de_AT")
		@Query(collation = "en_US")
		List<User> findWithMultipleCollationsFromAtQueryAndAtCollationByFirstname(String firstname);

		@ReadPreference("secondaryPreferred")
		List<User> findWithReadPreferenceFromAtReadPreferenceByFirstname(String firstname);

		@Query(readPreference = "secondaryPreferred")
		List<User> findWithReadPreferenceFromAtQueryByFirstname(String firstname);

		@ReadPreference("secondaryPreferred")
		@Query(readPreference = "primaryPreferred")
		List<User> findWithMultipleReadPreferencesFromAtQueryAndAtReadPreferenceByFirstname(String firstname);
	}

	interface SampleRepository extends Repository<Contact, Long> {

		List<Address> method();
	}

	interface SampleRepository2 extends Repository<Contact, Long> {

		List<Person> method();

		Customer methodReturningAnInterface();
	}

	interface InvalidUpdateMethodRepo extends Repository<Person, Long> {

		@org.springframework.data.mongodb.repository.Update
		void findAndUpdateByLastname(String lastname);

		@org.springframework.data.mongodb.repository.Update("{ '$inc' : { 'visits' : 1 } }")
		Person findAndIncrementVisitsByFirstname(String firstname);
	}

	interface InvalidAggregationMethodRepo extends Repository<Person, Long> {

		@Aggregation("[{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }]")
		List<User> findByAggregation();
	}

	interface Customer {

	}

}
