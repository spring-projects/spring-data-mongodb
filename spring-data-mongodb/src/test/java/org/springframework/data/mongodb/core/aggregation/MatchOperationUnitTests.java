package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

class MatchOperationUnitTests {
	
	@Test
	public void shouldRenderStdDevPopCorrectly() {
		MatchOperation operation = Aggregation.match().stdDevPop("size");
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
			isEqualTo(Document.parse("{ $match: { \"$expr\" : { \"$stdDevPop\" : \"$size\" } } } "));
		
	}
	
	@Test
	public void shouldRenderStdDevSampCorrectly() {
		MatchOperation operation = Aggregation.match().stdDevSamp("size");
		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).
			isEqualTo(Document.parse("{ $match: { \"$expr\" : { \"$stdDevSamp\" : \"$size\" } } } "));
		
	}

}
