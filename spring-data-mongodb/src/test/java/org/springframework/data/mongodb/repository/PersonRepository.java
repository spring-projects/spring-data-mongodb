/*
 * Copyright 2010-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Scroll;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.query.Param;
import org.springframework.lang.Nullable;

/**
 * Sample repository managing {@link Person} entities.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Fırat KÜÇÜK
 * @author Mark Paluch
 */
public interface PersonRepository extends MongoRepository<Person, String>, QuerydslPredicateExecutor<Person> {

	/**
	 * Returns all {@link Person}s with the given lastname.
	 *
	 * @param lastname
	 * @return
	 */
	List<Person> findByLastname(String lastname);

	List<Person> findByLastnameStartsWith(String prefix);

	List<Person> findByLastnameEndsWith(String postfix);

	/**
	 * Returns all {@link Person}s with the given lastname ordered by their firstname.
	 *
	 * @param lastname
	 * @return
	 */
	List<Person> findByLastnameOrderByFirstnameAsc(String lastname);

	/**
	 * Returns the {@link Person}s with the given firstname. Uses {@link Query} annotation to define the query to be
	 * executed.
	 *
	 * @param firstname
	 * @return
	 */
	@Query(value = "{ 'firstname' : ?0 }", fields = "{ 'firstname': 1, 'lastname': 1}")
	List<Person> findByThePersonsFirstname(String firstname);

	// DATAMONGO-871
	@Query(value = "{ 'firstname' : ?0 }")
	Person[] findByThePersonsFirstnameAsArray(String firstname);

	/**
	 * Returns all {@link Person}s with a firstname matching the given one (*-wildcard supported).
	 *
	 * @param firstname
	 * @return
	 */
	List<Person> findByFirstnameLike(@Nullable String firstname);

	List<Person> findByFirstnameNotContains(String firstname);

	/**
	 * Returns all {@link Person}s with a firstname not matching the given one (*-wildcard supported).
	 *
	 * @param firstname
	 * @return
	 */
	List<Person> findByFirstnameNotLike(String firstname);

	List<Person> findByFirstnameLikeOrderByLastnameAsc(String firstname, Sort sort);

	List<Person> findBySkillsContains(List<String> skills);

	List<Person> findBySkillsNotContains(List<String> skills);

	@Query("{'age' : { '$lt' : ?0 } }")
	List<Person> findByAgeLessThan(int age, Sort sort);

	/**
	 * Returns a scroll of {@link Person}s with a lastname matching the given one (*-wildcards supported).
	 *
	 * @param lastname
	 * @param scrollPosition
	 * @return
	 */
	Scroll<Person> findTop2ByLastnameLikeOrderByLastnameAscFirstnameAsc(String lastname,
			KeysetScrollPosition scrollPosition);

	/**
	 * Returns a scroll of {@link Person}s applying projections with a lastname matching the given one (*-wildcards
	 * supported).
	 *
	 * @param lastname
	 * @param pageable
	 * @return
	 */
	Scroll<PersonSummaryDto> findCursorProjectionByLastnameLike(String lastname, Pageable pageable);

	/**
	 * Returns a page of {@link Person}s with a lastname matching the given one (*-wildcards supported).
	 *
	 * @param lastname
	 * @param pageable
	 * @return
	 */
	Page<Person> findByLastnameLike(String lastname, Pageable pageable);

	@Query("{ 'lastname' : { '$regex' : '?0', '$options' : 'i'}}")
	Page<Person> findByLastnameLikeWithPageable(String lastname, Pageable pageable);

	List<Person> findByLastnameIgnoreCaseIn(String... lastname);

	/**
	 * Returns all {@link Person}s with a firstname contained in the given varargs.
	 *
	 * @param firstnames
	 * @return
	 */
	List<Person> findByFirstnameIn(String... firstnames);

	/**
	 * Returns all {@link Person}s with a firstname not contained in the given collection.
	 *
	 * @param firstnames
	 * @return
	 */
	List<Person> findByFirstnameNotIn(Collection<String> firstnames);

	List<Person> findByFirstnameAndLastname(String firstname, String lastname);

	/**
	 * Returns all {@link Person}s with an age between the two given values.
	 *
	 * @param from
	 * @param to
	 * @return
	 */
	List<Person> findByAgeBetween(int from, int to);

	/**
	 * Returns the {@link Person} with the given {@link Address} as shipping address.
	 *
	 * @param address
	 * @return
	 */
	Person findByShippingAddresses(Address address);

	/**
	 * Returns all {@link Person}s with the given {@link Address}.
	 *
	 * @param address
	 * @return
	 */
	List<Person> findByAddress(Address address);

	List<Person> findByAddressZipCode(String zipCode);

	List<Person> findByLastnameLikeAndAgeBetween(String lastname, int from, int to);

	List<Person> findByAgeOrLastnameLikeAndFirstnameLike(int age, String lastname, String firstname);

	List<Person> findByLocationNear(Point point);

	List<Person> findByLocationWithin(Circle circle);

	List<Person> findByLocationWithin(Box box);

	List<Person> findByLocationWithin(Polygon polygon);

	List<Person> findBySex(Sex sex);

	List<Person> findBySex(Sex sex, Pageable pageable);

	List<Person> findByNamedQuery(String firstname);

	GeoResults<Person> findByLocationNear(Point point, Distance maxDistance);

	// DATAMONGO-1110
	GeoResults<Person> findPersonByLocationNear(Point point, Range<Distance> distance);

	GeoPage<Person> findByLocationNear(Point point, Distance maxDistance, Pageable pageable);

	List<Person> findByCreator(User user);

	// DATAMONGO-425
	List<Person> findByCreatedAtLessThan(Date date);

	// DATAMONGO-425
	List<Person> findByCreatedAtGreaterThan(Date date);

	// DATAMONGO-425
	@Query("{ 'createdAt' : { '$lt' : ?0 }}")
	List<Person> findByCreatedAtLessThanManually(Date date);

	// DATAMONGO-427
	List<Person> findByCreatedAtBefore(Date date);

	// DATAMONGO-427
	List<Person> findByCreatedAtAfter(Date date);

	// DATAMONGO-472
	List<Person> findByLastnameNot(String lastname);

	// DATAMONGO-600
	List<Person> findByCredentials(Credentials credentials);

	// DATAMONGO-636
	long countByLastname(String lastname);

	// DATAMONGO-636
	int countByFirstname(String firstname);

	// DATAMONGO-636
	@Query(value = "{ 'lastname' : ?0 }", count = true)
	long someCountQuery(String lastname);

	// DATAMONGO-1454
	boolean existsByFirstname(String firstname);

	// DATAMONGO-1454
	@ExistsQuery(value = "{ 'lastname' : ?0 }")
	boolean someExistQuery(String lastname);

	// DATAMONGO-770
	List<Person> findByFirstnameIgnoreCase(@Nullable String firstName);

	// DATAMONGO-770
	List<Person> findByFirstnameNotIgnoreCase(String firstName);

	// DATAMONGO-770
	List<Person> findByFirstnameStartingWithIgnoreCase(String firstName);

	// DATAMONGO-770
	List<Person> findByFirstnameEndingWithIgnoreCase(String firstName);

	// DATAMONGO-770
	List<Person> findByFirstnameContainingIgnoreCase(String firstName);

	// DATAMONGO-870
	Slice<Person> findByAgeGreaterThan(int age, Pageable pageable);

	// DATAMONGO-821
	@Query("{ creator : { $exists : true } }")
	Page<Person> findByHavingCreator(Pageable page);

	// DATAMONGO-566
	List<Person> deleteByLastname(String lastname);

	// DATAMONGO-566
	Long deletePersonByLastname(String lastname);

	// DATAMONGO-1997
	Optional<Person> deleteOptionalByLastname(String lastname);

	// DATAMONGO-566
	@Query(value = "{ 'lastname' : ?0 }", delete = true)
	List<Person> removeByLastnameUsingAnnotatedQuery(String lastname);

	// DATAMONGO-566
	@Query(value = "{ 'lastname' : ?0 }", delete = true)
	Long removePersonByLastnameUsingAnnotatedQuery(String lastname);

	// DATAMONGO-893
	Page<Person> findByAddressIn(List<Address> address, Pageable page);

	// DATAMONGO-745
	@Query("{firstname:{$in:?0}, lastname:?1}")
	Page<Person> findByCustomQueryFirstnamesAndLastname(List<String> firstnames, String lastname, Pageable page);

	// DATAMONGO-745
	@Query("{lastname:?0, 'address.street':{$in:?1}}")
	Page<Person> findByCustomQueryLastnameAndAddressStreetInList(String lastname, List<String> streetNames,
			Pageable page);

	// DATAMONGO-950
	List<Person> findTop3ByLastnameStartingWith(String lastname);

	// DATAMONGO-950
	Page<Person> findTop3ByLastnameStartingWith(String lastname, Pageable pageRequest);

	// DATAMONGO-1865
	Person findFirstBy(); // limits to 1 result if more, just return the first one

	// DATAMONGO-1865
	Person findPersonByLastnameLike(String firstname); // single person, error if more than one

	// DATAMONGO-1865
	Optional<Person> findOptionalPersonByLastnameLike(String firstname); // optional still, error when more than one

	// DATAMONGO-1030
	PersonSummaryDto findSummaryByLastname(String lastname);

	PersonSummaryWithOptional findSummaryWithOptionalByLastname(String lastname);

	@Query("{ ?0 : ?1 }")
	List<Person> findByKeyValue(String key, String value);

	// DATAMONGO-1165
	@Query("{ firstname : { $in : ?0 }}")
	Stream<Person> findByCustomQueryWithStreamingCursorByFirstnames(List<String> firstnames);

	// DATAMONGO-990
	@Query("{ firstname : ?#{[0]}}")
	List<Person> findWithSpelByFirstnameForSpELExpressionWithParameterIndexOnly(String firstname);

	// DATAMONGO-990
	@Query("{ firstname : ?#{[0]}, email: ?#{principal.email} }")
	List<Person> findWithSpelByFirstnameAndCurrentUserWithCustomQuery(String firstname);

	// DATAMONGO-990
	@Query("{ firstname : :#{#firstname}}")
	List<Person> findWithSpelByFirstnameForSpELExpressionWithParameterVariableOnly(@Param("firstname") String firstname);

	// DATAMONGO-1911
	@Query("{ uniqueId: ?0}")
	Person findByUniqueId(UUID uniqueId);

	/**
	 * Returns the count of {@link Person} with the given firstname. Uses {@link CountQuery} annotation to define the
	 * query to be executed.
	 *
	 * @param firstname
	 * @return
	 */
	@CountQuery("{ 'firstname' : ?0 }") // DATAMONGO-1539
	long countByThePersonsFirstname(String firstname);

	/**
	 * Deletes {@link Person} entities with the given firstname. Uses {@link DeleteQuery} annotation to define the query
	 * to be executed.
	 *
	 * @param firstname
	 */
	@DeleteQuery("{ 'firstname' : ?0 }") // DATAMONGO-1539
	void deleteByThePersonsFirstname(String firstname);

	// DATAMONGO-1752
	Iterable<PersonExcerpt> findOpenProjectionBy();

	// DATAMONGO-1752
	Iterable<PersonSummary> findClosedProjectionBy();

	@Query(sort = "{ age : -1 }")
	List<Person> findByAgeGreaterThan(int age);

	@Query(sort = "{ age : -1 }")
	List<Person> findByAgeGreaterThan(int age, Sort sort);

	List<Person> findByFirstnameRegex(Pattern pattern);

	@Query(value = "{ 'id' : ?0 }", fields = "{ 'fans': { '$slice': [ ?1, ?2 ] } }")
	Person findWithSliceInProjection(String id, int skip, int limit);

	@Query(value = "{ 'id' : ?0 }", fields = "{ 'firstname': { '$toUpper': '$firstname' } }")
	Person findWithAggregationInProjection(String id);

	@Query(value = "{ 'shippingAddresses' : { '$elemMatch' : { 'city' : { '$eq' : 'lnz' } } } }",
			fields = "{ 'shippingAddresses.$': ?0 }")
	Person findWithArrayPositionInProjection(int position);

	@Query(value = "{ 'fans' : { '$elemMatch' : { '$ref' : 'user' } } }", fields = "{ 'fans.$': ?0 }")
	Person findWithArrayPositionInProjectionWithDbRef(int position);

	@Aggregation("{ '$project': { '_id' : '$lastname' } }")
	List<String> findAllLastnames();

	@Aggregation("{ '$project': { '_id' : '$lastname' } }")
	Stream<String> findAllLastnamesAsStream();

	@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
	List<PersonAggregate> groupByLastnameAnd(String property);

	@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
	Slice<PersonAggregate> groupByLastnameAndAsSlice(String property, Pageable pageable);

	@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
	List<PersonAggregate> groupByLastnameAnd(String property, Sort sort);

	@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
	List<PersonAggregate> groupByLastnameAnd(String property, Pageable page);

	@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
	int sumAge();

	@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
	AggregationResults<org.bson.Document> sumAgeAndReturnAggregationResultWrapper();

	@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
	AggregationResults<SumAge> sumAgeAndReturnAggregationResultWrapperWithConcreteType();

	@Query(value = "{_id:?0}")
	Optional<org.bson.Document> findDocumentById(String id);

	@Query(value = "{ 'firstname' : ?0, 'lastname' : ?1, 'email' : ?2 , 'age' : ?3, 'sex' : ?4, "
			+ "'createdAt' : ?5, 'skills' : ?6, 'address.street' : ?7, 'address.zipCode' : ?8, " //
			+ "'address.city' : ?9, 'uniqueId' : ?10, 'credentials.username' : ?11, 'credentials.password' : ?12 }")
	Person findPersonByManyArguments(String firstname, String lastname, String email, Integer age, Sex sex,
			Date createdAt, List<String> skills, String street, String zipCode, //
			String city, UUID uniqueId, String username, String password);

	List<Person> findByUnwrappedUserUsername(String username);

	List<Person> findByUnwrappedUser(User user);

	int findAndUpdateViaMethodArgAllByLastname(String lastname, UpdateDefinition update);

	@Update("{ '$inc' : { 'visits' : ?1 } }")
	int findAndIncrementVisitsByLastname(String lastname, int increment);

	@Query("{ 'lastname' : ?0 }")
	@Update("{ '$inc' : { 'visits' : ?1 } }")
	int updateAllByLastname(String lastname, int increment);

	@Update(pipeline = { "{ '$set' : { 'visits' : { '$add' : [ '$visits', ?1 ] } } }" })
	void findAndIncrementVisitsViaPipelineByLastname(String lastname, int increment);

	@Update("{ '$inc' : { 'visits' : ?#{[1]} } }")
	int findAndIncrementVisitsUsingSpELByLastname(String lastname, int increment);

	@Update("{ '$push' : { 'shippingAddresses' : ?1 } }")
	int findAndPushShippingAddressByEmail(String email, Address address);

	@Query("{ 'age' : null }")
	Person findByQueryWithNullEqualityCheck();

	List<Person> findBySpiritAnimal(User user);

}
