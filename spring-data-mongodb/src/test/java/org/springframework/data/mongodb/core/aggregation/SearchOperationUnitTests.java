package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;

import org.bson.Document;
import org.junit.jupiter.api.Test;

public class SearchOperationUnitTests {

	@Test
	public void shouldRenderSearchOperationWithAutocomplete() {

		Document map = new Document();
		map.append("path", "title").append("query", "off");
		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").autocomplete().withOptions(map)
				.build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$autocomplete\" : {\"path\" : \"title\", \"query\" : \"off\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithCompound() {

		Document map = new Document();
		Document textMap = new Document();
		textMap.put("query", "varieties");
		textMap.put("path", "description");
		ArrayList<Object> list = new ArrayList<Object>();
		list.add(textMap);
		map.append("must", list);

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").compound().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$compound\" : {\"must\" : [{\"path\" : \"description\", \"query\" : \"varieties\"}]}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithEquals() {

		Document map = new Document();
		map.append("path", "verifiedUser");
		map.append("value", true);

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").equals().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$equals\" : {\"path\" : \"verifiedUser\", \"value\" : true}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithExists() {

		Document map = new Document();
		map.append("path", "verifiedUser");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").exists().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(
				Document.parse("{\"$search\" : {\"$exists\" : {\"path\" : \"verifiedUser\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithGeoShape() {

		Document map = new Document();
		map.append("relation", "disjoint");
		map.append("path", "address.location");
		ArrayList<Double> values = new ArrayList<Double>();
		values.add(40d);
		values.add(5d);
		map.append("geometry", new Document().append("type", "Point").append("coordinates", values));

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").geoShape().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$geoShape\" : {\"geometry\" : {\"coordinates\" : [40.0, 5.0], \"type\" : \"Point\"}, \"path\" : \"address.location\", \"relation\" : \"disjoint\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithGeoWithin() {

		Document map = new Document();
		map.append("path", "address.location");
		ArrayList<Double> values = new ArrayList<Double>();
		values.add(40d);
		values.add(5d);
		map.append("geometry", new Document().append("type", "Polygon").append("coordinates", values));

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").geoWithin().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$geoWithin\" : {\"geometry\" : {\"coordinates\" : [40.0, 5.0], \"type\" : \"Polygon\"}, \"path\" : \"address.location\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithNear() {

		Document map = new Document();
		map.append("path", "runtime").append("origin", 279).append("pivot", 2);

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").near().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$near\" : {\"origin\" : 279, \"path\" : \"runtime\", \"pivot\" : 2}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithPhrase() {

		Document map = new Document();
		map.append("path", "title").append("query", "newyork");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").phrase().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$phrase\" : {\"path\" : \"title\", \"query\" : \"newyork\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithQueryString() {

		Document map = new Document();
		map.append("defaultPath", "title").append("query", "Rocky AND (IV OR 4 OR Four)");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").queryString().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$queryString\" : {\"defaultPath\" : \"title\", \"query\" : \"Rocky AND (IV OR 4 OR Four)\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithRange() {

		Document map = new Document();
		map.append("path", "runtime").append("gte", 2).append("lte", 3);

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").range().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$range\" : {\"gte\" : 2, \"lte\" : 3, \"path\" : \"runtime\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithRegex() {

		Document map = new Document();
		map.append("path", "title").append("query", "(.*) Seattle");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").regex().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\":{\"$regex\":{\"path\":\"title\", \"query\":\"(.*) Seattle\"}, \"index\":\"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithSpan() {

		Document map = new Document();
		map.append("operator",
				new Document().append("term", new Document("path", "description").append("query", "bunches")))
				.append("endPositionLte", 2);

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").span().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$span\" : {\"endPositionLte\" : 2, \"operator\" : {\"term\" : {\"path\" : \"description\", \"query\" : \"bunches\"}}}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithText() {

		Document map = new Document();
		map.append("path", "title").append("query", "surfer");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").text().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$text\" : {\"path\" : \"title\", \"query\" : \"surfer\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithWildcard() {

		Document map = new Document();
		map.append("path", "title").append("query", "Green D*");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").wildcard().withOptions(map).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$wildcard\" : {\"path\" : \"title\", \"query\" : \"Green D*\"}, \"index\" : \"ïndex\"}}"));

	}

	@Test
	public void shouldRenderSearchOperationWithHighlightOption() {

		Document map = new Document();
		map.append("path", "title").append("query", "variety");

		SearchOperation operation = SearchOperation.builder().withIndex("ïndex").text().withOptions(map)
				.withHighlight("description", 40).build();
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{\"$search\" : {\"$text\" : {\"path\" : \"title\", \"query\" : \"variety\"}, \"highlights\" : {\"maxCharsToExamine\" : 40, \"maxNumPassages\" : 5, \"path\" : \"description\"}, \"index\" : \"ïndex\"}}"));

	}

}
