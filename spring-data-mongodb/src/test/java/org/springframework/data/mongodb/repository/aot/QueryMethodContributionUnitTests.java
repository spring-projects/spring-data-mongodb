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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.assertThat;

import example.aot.User;
import example.aot.UserRepository;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import javax.lang.model.element.Modifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Vector;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.AotRepositoryFragmentMetadata;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.javapoet.ClassName;
import org.springframework.javapoet.FieldSpec;
import org.springframework.javapoet.MethodSpec;

/**
 * @author Christoph Strobl
 */
public class QueryMethodContributionUnitTests {

	@Test // GH-5004
	void rendersQueryForNearUsingPoint() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesNear", Point.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$near':?0}}") //
				.contains("arguments(location)") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersQueryForWithinUsingCircle() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesWithin", Circle.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$geoWithin':{'$center':?0}}") //
				.contains(
						"List.of(circle.getCenter().getX(), circle.getCenter().getY()), circle.getRadius().getNormalizedValue())") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersQueryForWithinUsingSphere() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesWithin", Sphere.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$geoWithin':{'$centerSphere':?0}}") //
				.contains(
						"List.of(circle.getCenter().getX(), circle.getCenter().getY()), circle.getRadius().getNormalizedValue())") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersQueryForWithinUsingBox() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesWithin", Box.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$geoWithin':{'$box':?0}}") //
				.contains("List.of(box.getFirst().getX(), box.getFirst().getY())") //
				.contains("List.of(box.getSecond().getX(), box.getSecond().getY())") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersQueryForWithinUsingPolygon() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesWithin", Polygon.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$geoWithin':{'$polygon':?0}}") //
				.contains("polygon.getPoints().stream().map(_p ->") //
				.contains("List.of(_p.getX(), _p.getY())") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersQueryForWithinUsingGeoJsonPolygon() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesWithin", GeoJsonPolygon.class);

		assertThat(methodSpec.toString()) //
				.contains("{'location.coordinates':{'$geoWithin':{'$geometry':?0}}") //
				.contains("arguments(polygon)") //
				.contains("return finder.matching(filterQuery).all()");
	}

	@Test // GH-5004
	void rendersNearQueryForGeoResults() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepoWithMeta.class, "findByLocationCoordinatesNear", Point.class,
				Distance.class);

		assertThat(methodSpec.toString()) //
				.contains("NearQuery.near(point)") //
				.contains("nearQuery.maxDistance(maxDistance).in(maxDistance.getMetric())") //
				.contains(".withReadPreference(com.mongodb.ReadPreference.valueOf(\"NEAREST\")") //
				.doesNotContain("nearQuery.query(") //
				.contains(".near(nearQuery)") //
				.contains("return nearFinder.all()");
	}

	@Test // GH-5004
	void rendersNearQueryWithDistanceRangeForGeoResults() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesNear", Point.class, Range.class);

		assertThat(methodSpec.toString()) //
				.contains("NearQuery.near(point)") //
				.contains("if(distance.getLowerBound().isBounded())") //
				.contains("nearQuery.minDistance(min).in(min.getMetric())") //
				.contains("if(distance.getUpperBound().isBounded())") //
				.contains("nearQuery.maxDistance(max).in(max.getMetric())") //
				.contains(".near(nearQuery)") //
				.contains("return nearFinder.all()");
	}

	@Test // GH-5004
	void rendersNearQueryReturningGeoPage() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesNear", Point.class, Distance.class,
				Pageable.class);

		assertThat(methodSpec.toString()) //
				.contains("NearQuery.near(point)") //
				.contains("nearQuery.maxDistance(maxDistance).in(maxDistance.getMetric())") //
				.doesNotContain("nearQuery.query(") //
				.contains("var geoResult = nearFinder.all()") //
				.contains("PageableExecutionUtils.getPage(geoResult.getContent(), pageable, () -> nearFinder.count())")
				.contains("GeoPage<>(geoResult, pageable, resultPage.getTotalElements())");
	}

	@Test // GH-5004
	void rendersNearQueryWithFilterForGeoResults() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByLocationCoordinatesNearAndLastname", Point.class,
				Distance.class, String.class);

		assertThat(methodSpec.toString()) //
				.contains("NearQuery.near(point)") //
				.contains("nearQuery.maxDistance(maxDistance).in(maxDistance.getMetric())") //
				.contains("filterQuery = createQuery(\"{'lastname':?0}\", arguments(lastname))") //
				.contains("nearQuery.query(filterQuery)") //
				.contains(".near(nearQuery)") //
				.contains("return nearFinder.all()");
	}

	@Test // GH-5006
	void rendersExpressionUsingParameterIndex() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findWithExpressionUsingParameterIndex", String.class);

		assertThat(methodSpec.toString()) //
				.contains("createQuery(\"{ firstname : ?#{[0]} }\", argumentMap(\"firstname\", firstname))");
	}

	@Test // GH-5006
	void rendersExpressionUsingParameterName() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findWithExpressionUsingParameterName", String.class);

		assertThat(methodSpec.toString()) //
				.contains("createQuery(\"{ firstname : :#{#firstname} }\", argumentMap(\"firstname\", firstname))");
	}

	@Test // GH-4939
	void rendersRegexCriteria() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "findByFirstnameRegex", Pattern.class);

		assertThat(methodSpec.toString()) //
				.contains("createQuery(\"{'firstname':{'$regex':?0}}\", arguments(pattern))");
	}

	@Test // GH-4939
	void rendersHint() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepoWithMeta.class, "findByFirstname", String.class);

		assertThat(methodSpec.toString()) //
				.contains(".withHint(\"fn-idx\")");
	}

	@Test // GH-4939
	void rendersCollation() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepoWithMeta.class, "findByFirstname", String.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence(".collation(", "Collation.parse(\"en_US\"))");
	}

	@Test // GH-4939
	void rendersCollationFromExpression() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepoWithMeta.class, "findWithCollationByFirstname", String.class, String.class);

		assertThat(methodSpec.toString()) //
				.containsIgnoringWhitespaces(
						"collationOf(evaluate(\"?#{[1]}\", argumentMap(\"firstname\", firstname, \"locale\", locale)))");
	}

	@Test
	void rendersVectorSearchFilterFromAnnotatedQuery() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "annotatedVectorSearch", String.class, Vector.class,
				Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence("$vectorSearch =",
						"Aggregation.vectorSearch(\"embedding.vector_cos\").path(\"embedding\").vector(vector).limit(limit);")
				.contains("filter = createQuery(\"{lastname: ?0}\", arguments(lastname, distance))")
				.contains("$vectorSearch.filter(filter.getQueryObject())");
	}

	@Test
	void rendersVectorSearchNumCandidatesExpression() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "annotatedVectorSearch", String.class, Vector.class,
				Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence("$vectorSearch.numCandidates",
						"evaluate(\"#{10+10}\", argumentMap(\"lastname\", lastname, \"distance\", distance)))");
	}

	@Test
	void rendersVectorSearchScoringFunctionFromScore() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "annotatedVectorSearch", String.class, Vector.class,
				Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.contains("ScoringFunction scoringFunction = distance.getFunction()");
	}

	@Test
	void rendersVectorSearchSearchTypeFromAnnotation() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "annotatedVectorSearch", String.class, Vector.class,
				Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence("$vectorSearch.searchType(", "VectorSearchOperation.SearchType.ANN)");
	}

	@Test
	void rendersVectorSearchQueryFromMethodName() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "searchCosineByLastnameAndEmbeddingNear", String.class,
				Vector.class, Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.contains("filter = createQuery(\"{'lastname':?0}\", arguments(lastname, similarity))");
	}

	@Test
	void rendersVectorSearchNumCandidatesFromLimitIfNotExplicitlyDefined() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "searchCosineByLastnameAndEmbeddingNear", String.class,
				Vector.class, Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.contains("$vectorSearch.numCandidates(limit.max() * 20)");
	}

	@Test
	void rendersVectorSearchLimitFromAnnotation() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "searchByLastnameAndEmbeddingWithin", String.class,
				Vector.class, Range.class);

		assertThat(methodSpec.toString()) //
				.contains("Aggregation.vectorSearch(\"embedding.vector_cos\").path(\"embedding\").vector(vector).limit(10)")
				.contains("$vectorSearch.numCandidates(10 * 20)");
	}

	@Test
	void rendersVectorSearchLimitFromExpression() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepoWithMeta.class,
				"searchWithLimitAsExpressionByLastnameAndEmbeddingWithinOrderByFirstname", String.class, Vector.class,
				Range.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence(
						"Aggregation.vectorSearch(\"embedding.vector_cos\").path(\"embedding\").vector(vector).limit(",
						"evaluate(\"#{5+5}\", argumentMap(\"lastname\", lastname, \"distance\", distance)")
				.containsSubsequence("$vectorSearch.numCandidates(",
						"evaluate(\"#{5+5}\", argumentMap(\"lastname\", lastname, \"distance\", distance))) * 20)");
	}

	@Test
	void rendersVectorSearchOrderByScoreAsDefault() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "searchCosineByLastnameAndEmbeddingNear", String.class,
				Vector.class, Score.class, Limit.class);

		assertThat(methodSpec.toString()) //
				.contains("$vectorSearch.withSearchScore(\"__score__\")")
				.containsSubsequence("$sort = ", "Aggregation.sort(", "DESC, \"__score__\")")
				.containsSubsequence("AggregationPipeline(", "List.of($vectorSearch, $sort))");
	}

	@Test
	void rendersVectorSearchOrderByWithScoreLast() throws NoSuchMethodException {

		MethodSpec methodSpec = codeOf(UserRepository.class, "searchByLastnameAndEmbeddingWithinOrderByFirstname",
				String.class, Vector.class, Range.class);

		assertThat(methodSpec.toString()) //
				.containsSubsequence("AggregationOperation $sort = (_ctx) -> {", //
						"_mappedSort = _ctx.getMappedObject(", //
						"Document.parse(\"{'firstname':{'$numberInt':'1'}}\")", //
						"Document(\"$sort\", _mappedSort.append(\"__score__\", -1))");
	}

	private static MethodSpec codeOf(Class<?> repository, String methodName, Class<?>... args)
			throws NoSuchMethodException {

		Method method = repository.getMethod(methodName, args);

		TestMongoAotRepositoryContext repoContext = new TestMongoAotRepositoryContext(repository, null);
		MongoRepositoryContributor contributor = new MongoRepositoryContributor(repoContext);
		MethodContributor<? extends QueryMethod> methodContributor = contributor.contributeQueryMethod(method);

		if (methodContributor == null) {
			Assertions.fail("No contribution for method %s.%s(%s)".formatted(repository.getSimpleName(), methodName,
					Arrays.stream(args).map(Class::getSimpleName).toList()));
		}
		AotRepositoryFragmentMetadata metadata = new AotRepositoryFragmentMetadata(ClassName.get(repository));
		metadata.addField(
				FieldSpec.builder(MongoOperations.class, "mongoOperations", Modifier.PRIVATE, Modifier.FINAL).build());

		TestQueryMethodGenerationContext methodContext = new TestQueryMethodGenerationContext(
				repoContext.getRepositoryInformation(), method, methodContributor.getQueryMethod(), metadata);
		return methodContributor.contribute(methodContext);
	}

	static class TestQueryMethodGenerationContext extends AotQueryMethodGenerationContext {

		protected TestQueryMethodGenerationContext(RepositoryInformation repositoryInformation, Method method,
				QueryMethod queryMethod, AotRepositoryFragmentMetadata targetTypeMetadata) {
			super(repositoryInformation, method, queryMethod, targetTypeMetadata);
		}
	}

	interface UserRepoWithMeta extends Repository<User, String> {

		@Hint("fn-idx")
		@Collation("en_US")
		List<User> findByFirstname(String firstname);

		@Collation("?#{[1]}")
		List<User> findWithCollationByFirstname(String firstname, String locale);

		@ReadPreference("NEAREST")
		GeoResults<User> findByLocationCoordinatesNear(Point point, Distance maxDistance);

		@VectorSearch(indexName = "embedding.vector_cos", limit = "#{5+5}")
		SearchResults<User> searchWithLimitAsExpressionByLastnameAndEmbeddingWithinOrderByFirstname(String lastname,
				Vector vector, Range<Similarity> distance);
	}
}
