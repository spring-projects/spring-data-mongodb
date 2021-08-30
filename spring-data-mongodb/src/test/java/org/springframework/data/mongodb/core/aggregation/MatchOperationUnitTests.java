package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

class MatchOperationUnitTests {
	
	@Test // DATAMONGO - 3729
	public void shouldRenderStdDevPopCorrectly() {
		MatchOperation operation = Aggregation.match().withValueOf(ArithmeticOperators.valueOf("quiz").stdDevPop());
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
			isEqualTo(Document.parse("{ $match: { \"$expr\" : { \"$stdDevPop\" : \"$quiz\" } } } "));
		
	}
	
	@Test // DATAMONGO - 3729
	public void shouldRenderStdDevSampCorrectly() {
		MatchOperation operation = Aggregation.match().withValueOf(ArithmeticOperators.valueOf("quiz").stdDevSamp());
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
			isEqualTo(Document.parse("{ $match: { \"$expr\" : { \"$stdDevSamp\" : \"$quiz\" } } } "));
		
	}

}
