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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit test to reproduce DATAMONGO-273.
 *
 * @author Harlan Iverson
 * @author Oliver Gierke
 */
public class DataMongo273Tests {

	MappingMongoConverter converter;

	@BeforeEach
	public void setupMongoConverter() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		DbRefResolver factory = mock(DbRefResolver.class);

		converter = new MappingMongoConverter(factory, mappingContext);
		converter.afterPropertiesSet();
	}

	@Test // DATAMONGO-273
	public void convertMapOfThings() {

		Plane plane = new Plane("Boeing", 4);
		Train train = new Train("Santa Fe", 200);
		Automobile automobile = new Automobile("Tesla", "Roadster", 2);

		Map<String, Object> mapOfThings = new HashMap<String, Object>();
		mapOfThings.put("plane", plane);
		mapOfThings.put("train", train);
		mapOfThings.put("automobile", automobile);

		Document result = new Document();
		converter.write(mapOfThings, result);

		@SuppressWarnings("unchecked")
		Map<String, Object> mapOfThings2 = converter.read(Map.class, result);

		assertThat(mapOfThings2.get("plane") instanceof Plane).isTrue();
		assertThat(mapOfThings2.get("train") instanceof Train).isTrue();
		assertThat(mapOfThings2.get("automobile") instanceof Automobile).isTrue();
	}

	@Test // DATAMONGO-294
	@Disabled("TODO: Mongo3 - this is no longer supported as DBList is no Bson type :/")
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void convertListOfThings() {
		Plane plane = new Plane("Boeing", 4);
		Train train = new Train("Santa Fe", 200);
		Automobile automobile = new Automobile("Tesla", "Roadster", 2);

		List listOfThings = new ArrayList();
		listOfThings.add(plane);
		listOfThings.add(train);
		listOfThings.add(automobile);

		Document result = new Document();
		converter.write(listOfThings, result);

		List listOfThings2 = converter.read(List.class, result);

		assertThat(listOfThings2.get(0) instanceof Plane).isTrue();
		assertThat(listOfThings2.get(1) instanceof Train).isTrue();
		assertThat(listOfThings2.get(2) instanceof Automobile).isTrue();
	}

	@Test // DATAMONGO-294
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void convertListOfThings_NestedInMap() {

		Plane plane = new Plane("Boeing", 4);
		Train train = new Train("Santa Fe", 200);
		Automobile automobile = new Automobile("Tesla", "Roadster", 2);

		List listOfThings = new ArrayList();
		listOfThings.add(plane);
		listOfThings.add(train);
		listOfThings.add(automobile);

		Map<String, Object> box = new HashMap<String, Object>();
		box.put("one", listOfThings);

		Shipment shipment = new Shipment(box);

		Document result = new Document();
		converter.write(shipment, result);

		Shipment shipment2 = converter.read(Shipment.class, result);

		List listOfThings2 = (List) shipment2.getBoxes().get("one");

		assertThat(listOfThings2.get(0) instanceof Plane).isTrue();
		assertThat(listOfThings2.get(1) instanceof Train).isTrue();
		assertThat(listOfThings2.get(2) instanceof Automobile).isTrue();
	}

	static class Plane {

		String maker;
		int numberOfPropellers;

		public Plane(String maker, int numberOfPropellers) {
			this.maker = maker;
			this.numberOfPropellers = numberOfPropellers;
		}
	}

	static class Train {

		String railLine;
		int numberOfCars;

		public Train(String railLine, int numberOfCars) {
			this.railLine = railLine;
			this.numberOfCars = numberOfCars;
		}
	}

	static class Automobile {

		String make;
		String model;
		int numberOfDoors;

		public Automobile(String make, String model, int numberOfDoors) {
			this.make = make;
			this.model = model;
			this.numberOfDoors = numberOfDoors;
		}
	}

	@SuppressWarnings("rawtypes")
	static class Shipment {

		Map<String, Object> boxes;

		public Shipment(Map<String, Object> boxes) {
			this.boxes = boxes;
		}

		public Map getBoxes() {
			return boxes;
		}
	}
}
