package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.springframework.data.core.TypedPropertyPath;

class CriteriaWithPropertyReferenceUnitTests {

	static Criteria base = new Criteria("name").is("Bubba");

	static List<Fixture> compare = List.of( //
			new Fixture( //
					"constructor", //
					Criteria.where((TestEntity e) -> e.name), //
					new Criteria("name") //
			), //
			new Fixture( //
					"path", //
					Criteria.where(TypedPropertyPath.ofReference((TestEntity e) -> e.referenced).then(r -> r.value)), //
					new Criteria("referenced.value") //
			), //
			new Fixture( //
					"where", //
					Criteria.where((TestEntity e) -> e.name), //
					new Criteria("name") //
			), //
			new Fixture( //
					"and", //
					base.and((TestEntity e) -> e.age), //
					base.and("age") //
			) //
	);

	@ParameterizedTest
	@FieldSource
	void compare(Fixture fixture) {
		assertThat(fixture.underTest).describedAs(fixture.description).isEqualTo(fixture.expected);
	}

	record Fixture(String description, Criteria underTest, Criteria expected) {
	}

	record TestEntity(String name, Long age, Referenced referenced) {
	}
	record Referenced(String value) {}
}
