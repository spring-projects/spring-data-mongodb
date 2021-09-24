package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;



import org.bson.Document;
import org.junit.jupiter.api.Test;


import java.util.HashMap;
import java.util.Map;

public class SearchOperationUnitTests {
	
	@Test
	public void shouldRenderSearchOperationWithAutocomplete() {

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("path", "title");
		map.put("query", "off");
		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").autocomplete().withOptions(map)
				.build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$autocomplete\" : {\"path\" : \"title\", \"query\" : \"off\"}, \"index\" : \"ïndex\"}}"));

	}

}
