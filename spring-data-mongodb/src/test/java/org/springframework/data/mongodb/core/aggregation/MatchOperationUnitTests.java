package org.springframework.data.mongodb.core.aggregation;


import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link MatchOperation}.
 *
 * @author Divya Srivastava
 */
class MatchOperationUnitTests {
	
	@Test // GH-3790
	void matchShouldRenderCorrectly() {

		MatchOperation operation = Aggregation.match(ArithmeticOperators.valueOf("quiz").stdDevPop());
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
		isEqualTo("{ $match: { \"$stdDevPop\" : \"$quiz\" } } ");
	}

}
