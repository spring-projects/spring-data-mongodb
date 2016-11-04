/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.convert.GeoConverters.BoxToDocumentConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.CircleToDocumentConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToBoxConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToCircleConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToPointConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToPolygonConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToSphereConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.GeoCommandToDocumentConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.PointToDocumentConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.PolygonToDocumentConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.SphereToDocumentConverter;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.query.GeoCommand;

/**
 * Unit tests for {@link GeoConverters}.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.5
 */
public class GeoConvertersUnitTests {

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsBoxToDocumentAndBackCorrectly() {

		Box box = new Box(new Point(1, 2), new Point(3, 4));

		Document document = BoxToDocumentConverter.INSTANCE.convert(box);
		Box result = DocumentToBoxConverter.INSTANCE.convert(document);

		assertThat(result, is(box));
		assertThat(result.getClass().equals(Box.class), is(true));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsCircleToDocumentAndBackCorrectlyNeutralDistance() {

		Circle circle = new Circle(new Point(1, 2), 3);

		Document document = CircleToDocumentConverter.INSTANCE.convert(circle);
		Circle result = DocumentToCircleConverter.INSTANCE.convert(document);

		assertThat(result, is(circle));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsCircleToDocumentAndBackCorrectlyMilesDistance() {

		Distance radius = new Distance(3, Metrics.MILES);
		Circle circle = new Circle(new Point(1, 2), radius);

		Document document = CircleToDocumentConverter.INSTANCE.convert(circle);
		Circle result = DocumentToCircleConverter.INSTANCE.convert(document);

		assertThat(result, is(circle));
		assertThat(result.getRadius(), is(radius));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsPolygonToDocumentAndBackCorrectly() {

		Polygon polygon = new Polygon(new Point(1, 2), new Point(2, 3), new Point(3, 4), new Point(5, 6));

		Document document = PolygonToDocumentConverter.INSTANCE.convert(polygon);
		Polygon result = DocumentToPolygonConverter.INSTANCE.convert(document);

		assertThat(result, is(polygon));
		assertThat(result.getClass().equals(Polygon.class), is(true));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsSphereToDocumentAndBackCorrectlyWithNeutralDistance() {

		Sphere sphere = new Sphere(new Point(1, 2), 3);

		Document document = SphereToDocumentConverter.INSTANCE.convert(sphere);
		Sphere result = DocumentToSphereConverter.INSTANCE.convert(document);

		assertThat(result, is(sphere));
		assertThat(result.getClass().equals(Sphere.class), is(true));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsSphereToDocumentAndBackCorrectlyWithKilometerDistance() {

		Distance radius = new Distance(3, Metrics.KILOMETERS);
		Sphere sphere = new Sphere(new Point(1, 2), radius);

		Document document = SphereToDocumentConverter.INSTANCE.convert(sphere);
		Sphere result = DocumentToSphereConverter.INSTANCE.convert(document);

		assertThat(result, is(sphere));
		assertThat(result.getRadius(), is(radius));
		assertThat(result.getClass().equals(org.springframework.data.mongodb.core.geo.Sphere.class), is(true));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsPointToListAndBackCorrectly() {

		Point point = new Point(1, 2);

		Document document = PointToDocumentConverter.INSTANCE.convert(point);
		Point result = DocumentToPointConverter.INSTANCE.convert(document);

		assertThat(result, is(point));
		assertThat(result.getClass().equals(Point.class), is(true));
	}

	/**
	 * @see DATAMONGO-858
	 */
	@Test
	public void convertsGeoCommandToDocumentCorrectly() {

		Box box = new Box(new double[] { 1, 2 }, new double[] { 3, 4 });
		GeoCommand cmd = new GeoCommand(box);

		Document document = GeoCommandToDocumentConverter.INSTANCE.convert(cmd);

		assertThat(document, is(notNullValue()));

		List<Object> boxObject = (List<Object>) document.get("$box");

		assertThat(boxObject,
				is((Object) Arrays.asList(GeoConverters.toList(box.getFirst()), GeoConverters.toList(box.getSecond()))));
	}
}
