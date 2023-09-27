/*
 * Copyright 2016-2023 the original author or authors.
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

import org.springframework.data.mongodb.repository.query.MongoQueryMethodUnitTests.PersonRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.User;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
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
 * Unit test for {@link ReactiveMongoQueryMethod}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jorge Rodríguez
 */
public class ReactiveMongoQueryMethodUnitTests {

	MongoMappingContext context;

	@BeforeEach
	public void setUp() {
		context = new MongoMappingContext();
	}

	@Test // DATAMONGO-1444
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		ReactiveMongoQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		MongoEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Address.class);
		assertThat(metadata.getCollectionName()).isEqualTo("contact");
	}

	@Test // DATAMONGO-1444
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(SampleRepository2.class, "method");
		MongoEntityMetadata<?> entityInformation = queryMethod.getEntityInformation();

		assertThat(entityInformation.getJavaType()).isAssignableFrom(Person.class);
		assertThat(entityInformation.getCollectionName()).isEqualTo("person");
	}

	@Test // DATAMONGO-1444
	public void discoversUserAsDomainTypeForGeoPagingQueryMethod() throws Exception {

		MongoQueryMethod queryMethod = queryMethod(PersonRepository.class, "findByLocationNear", Point.class,
				Distance.class, Pageable.class);
		assertThat(queryMethod.isGeoNearQuery()).isFalse();
		assertThat(queryMethod.isPageQuery()).isFalse();

		queryMethod = queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class);
		assertThat(queryMethod.isGeoNearQuery()).isFalse();
		assertThat(queryMethod.isPageQuery()).isFalse();
		assertThat(queryMethod.getEntityInformation().getJavaType()).isAssignableFrom(User.class);

		assertThat(queryMethod(PersonRepository.class, "findByEmailAddress", String.class, Point.class).isGeoNearQuery())
				.isTrue();
		assertThat(queryMethod(PersonRepository.class, "findByFirstname", String.class, Point.class).isGeoNearQuery())
				.isFalse();
		assertThat(queryMethod(PersonRepository.class, "findByLastname", String.class, Point.class).isGeoNearQuery())
				.isTrue();
	}

	@Test // DATAMONGO-1444
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findByFirstname", String.class, Point.class);

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new MongoQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), null));
	}

	@Test // DATAMONGO-1444
	public void rejectsMonoPageableResult() {
		assertThatIllegalStateException()
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoByLastname", String.class, Pageable.class).verify());
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodObjectForMethodReturningAnInterface() throws Exception {
		queryMethod(SampleRepository2.class, "methodReturningAnInterface");
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithEmptyMetaCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "emptyMetaAnnotation");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().hasValues()).isFalse();
	}

	@Test // DATAMONGO-1444
	public void createsMongoQueryMethodWithMaxExecutionTimeCorrectly() throws Exception {

		MongoQueryMethod method = queryMethod(PersonRepository.class, "metaWithMaxExecutionTime");

		assertThat(method.hasQueryMetaAttributes()).isTrue();
		assertThat(method.getQueryMetaAttributes().getMaxTimeMsec()).isEqualTo(100L);
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionOnWrappedPage() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoPageByLastname", String.class, Pageable.class).verify());
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionOnWrappedSlice() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoSliceByLastname", String.class, Pageable.class).verify());
	}

	@Test // DATAMONGO-1444
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(User.class);
	}

	@Test // DATAMONGO-2153
	public void findsAnnotatedAggregation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregation");

		Assertions.assertThat(method.hasAnnotatedAggregation()).isTrue();
		Assertions.assertThat(method.getAnnotatedAggregation()).hasSize(1);
	}

	@Test // DATAMONGO-2153
	public void detectsCollationForAggregation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findByAggregationWithCollation");

		Assertions.assertThat(method.hasAnnotatedCollation()).isTrue();
		Assertions.assertThat(method.getAnnotatedCollation()).isEqualTo("de_AT");
	}

	@Test // GH-2107
	public void queryCreationFailsOnInvalidUpdate() throws Exception {

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> queryMethod(InvalidUpdateMethodRepo.class, "findAndUpdateByLastname", String.class).verify()) //
				.withMessageContaining("Update") //
				.withMessageContaining("findAndUpdateByLastname");
	}

	@Test // GH-2107
	public void queryCreationForUpdateMethodFailsOnInvalidReturnType() throws Exception {

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> queryMethod(InvalidUpdateMethodRepo.class, "findAndIncrementVisitsByFirstname", String.class).verify()) //
				.withMessageContaining("Update") //
				.withMessageContaining("numeric") //
				.withMessageContaining("findAndIncrementVisitsByFirstname");
	}

	@Test // GH-3002
	void readsCollationFromAtCollationAnnotation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(MongoQueryMethodUnitTests.PersonRepository.class, "findWithCollationFromAtCollationByFirstname", String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("en_US");
	}

	@Test // GH-3002
	void readsCollationFromAtQueryAnnotation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(MongoQueryMethodUnitTests.PersonRepository.class, "findWithCollationFromAtQueryByFirstname", String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("en_US");
	}

	@Test // GH-3002
	void annotatedCollationClashSelectsAtCollationAnnotationValue() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findWithMultipleCollationsFromAtQueryAndAtCollationByFirstname", String.class);

		assertThat(method.hasAnnotatedCollation()).isTrue();
		assertThat(method.getAnnotatedCollation()).isEqualTo("de_AT");
	}


	@Test // GH-2971
	void readsReadPreferenceAtQueryAnnotation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findWithReadPreferenceFromAtReadPreferenceByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void readsReadPreferenceFromAtQueryAnnotation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findWithReadPreferenceFromAtQueryByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void annotatedReadPreferenceClashSelectsAtReadPreferenceAnnotationValue() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "findWithMultipleReadPreferencesFromAtQueryAndAtReadPreferenceByFirstname", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	@Test // GH-2971
	void readsReadPreferenceAtRepositoryAnnotation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("primaryPreferred");
	}

	@Test // GH-2971
	void detectsReadPreferenceForAggregation() throws Exception {

		ReactiveMongoQueryMethod method = queryMethod(MongoQueryMethodUnitTests.PersonRepository.class, "findByAggregationWithReadPreference");

		assertThat(method.hasAnnotatedReadPreference()).isTrue();
		assertThat(method.getAnnotatedReadPreference()).isEqualTo("secondaryPreferred");
	}

	private ReactiveMongoQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters)
			throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new ReactiveMongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	@ReadPreference(value = "primaryPreferred")
	interface PersonRepository extends Repository<User, Long> {

		Mono<Person> findMonoByLastname(String lastname, Pageable pageRequest);

		Mono<Page<Person>> findMonoPageByLastname(String lastname, Pageable pageRequest);

		Mono<Slice<Person>> findMonoSliceByLastname(String lastname, Pageable pageRequest);

		// Misses Pageable
		Flux<User> findByLocationNear(Point point, Distance distance);

		Flux<User> findByLocationNear(Point point, Distance distance, Pageable pageable);

		Mono<GeoResult<User>> findByEmailAddress(String lastname, Point location);

		Flux<User> findByFirstname(String firstname, Point location);

		Flux<GeoResult<User>> findByLastname(String lastname, Point location);

		@Meta
		Flux<User> emptyMetaAnnotation();

		@Meta(maxExecutionTimeMs = 100)
		Flux<User> metaWithMaxExecutionTime();

		void deleteByUserName(String userName);

		@Aggregation("{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }")
		Flux<User> findByAggregation();

		@Aggregation(pipeline = "{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }",
				collation = "de_AT")
		Flux<User> findByAggregationWithCollation();

		@Aggregation(pipeline = "{'$group': { _id: '$templateId', maxVersion : { $max : '$version'} } }", readPreference = "secondaryPreferred")
		Flux<User> findByAggregationWithReadPreference();

		@Collation("en_US")
		List<User> findWithCollationFromAtCollationByFirstname(String firstname);

		@Query(collation = "en_US")
		List<User> findWithCollationFromAtQueryByFirstname(String firstname);

		@Collation("de_AT")
		@Query(collation = "en_US")
		List<User> findWithMultipleCollationsFromAtQueryAndAtCollationByFirstname(String firstname);

		@ReadPreference("secondaryPreferred")
		Flux<User> findWithReadPreferenceFromAtReadPreferenceByFirstname(String firstname);

		@Query(readPreference = "secondaryPreferred")
		Flux<User> findWithReadPreferenceFromAtQueryByFirstname(String firstname);

		@ReadPreference("secondaryPreferred")
		@Query(readPreference = "primaryPreferred")
		Flux<User> findWithMultipleReadPreferencesFromAtQueryAndAtReadPreferenceByFirstname(String firstname);
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
		Mono<Void> findAndUpdateByLastname(String lastname);

		@org.springframework.data.mongodb.repository.Update("{ '$inc' : { 'visits' : 1 } }")
		Mono<Person> findAndIncrementVisitsByFirstname(String firstname);
	}

	interface Customer {}
}
