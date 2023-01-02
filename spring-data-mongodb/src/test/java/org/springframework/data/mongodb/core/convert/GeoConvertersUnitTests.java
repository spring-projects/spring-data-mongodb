/*
 * Copyright 2014-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.convert.GeoConverters.*;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.query.GeoCommand;

/**
 * Unit tests for {@link GeoConverters}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.5
 */
public class GeoConvertersUnitTests {

	@Test // DATAMONGO-858
	public void convertsBoxToDocumentAndBackCorrectly() {

		Box box = new Box(new Point(1, 2), new Point(3, 4));

		Document document = BoxToDocumentConverter.INSTANCE.convert(box);
		Box result = DocumentToBoxConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(box);
		assertThat(result.getClass().equals(Box.class)).isTrue();
	}

	@Test // DATAMONGO-858
	public void convertsCircleToDocumentAndBackCorrectlyNeutralDistance() {

		Circle circle = new Circle(new Point(1, 2), 3);

		Document document = CircleToDocumentConverter.INSTANCE.convert(circle);
		Circle result = DocumentToCircleConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(circle);
	}

	@Test // DATAMONGO-858
	public void convertsCircleToDocumentAndBackCorrectlyMilesDistance() {

		Distance radius = new Distance(3, Metrics.MILES);
		Circle circle = new Circle(new Point(1, 2), radius);

		Document document = CircleToDocumentConverter.INSTANCE.convert(circle);
		Circle result = DocumentToCircleConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(circle);
		assertThat(result.getRadius()).isEqualTo(radius);
	}

	@Test // DATAMONGO-858
	public void convertsPolygonToDocumentAndBackCorrectly() {

		Polygon polygon = new Polygon(new Point(1, 2), new Point(2, 3), new Point(3, 4), new Point(5, 6));

		Document document = PolygonToDocumentConverter.INSTANCE.convert(polygon);
		Polygon result = DocumentToPolygonConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(polygon);
		assertThat(result.getClass().equals(Polygon.class)).isTrue();
	}

	@Test // DATAMONGO-858
	public void convertsSphereToDocumentAndBackCorrectlyWithNeutralDistance() {

		Sphere sphere = new Sphere(new Point(1, 2), 3);

		Document document = SphereToDocumentConverter.INSTANCE.convert(sphere);
		Sphere result = DocumentToSphereConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(sphere);
		assertThat(result.getClass().equals(Sphere.class)).isTrue();
	}

	@Test // DATAMONGO-858
	public void convertsSphereToDocumentAndBackCorrectlyWithKilometerDistance() {

		Distance radius = new Distance(3, Metrics.KILOMETERS);
		Sphere sphere = new Sphere(new Point(1, 2), radius);

		Document document = SphereToDocumentConverter.INSTANCE.convert(sphere);
		Sphere result = DocumentToSphereConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(sphere);
		assertThat(result.getRadius()).isEqualTo(radius);
		assertThat(result.getClass().equals(Sphere.class)).isTrue();
	}

	@Test // DATAMONGO-858
	public void convertsPointToListAndBackCorrectly() {

		Point point = new Point(1, 2);

		Document document = PointToDocumentConverter.INSTANCE.convert(point);
		Point result = DocumentToPointConverter.INSTANCE.convert(document);

		assertThat(result).isEqualTo(point);
		assertThat(result.getClass().equals(Point.class)).isTrue();
	}

	@Test // DATAMONGO-858
	public void convertsGeoCommandToDocumentCorrectly() {

		Box box = new Box(new double[] { 1, 2 }, new double[] { 3, 4 });
		GeoCommand cmd = new GeoCommand(box);

		Document document = GeoCommandToDocumentConverter.INSTANCE.convert(cmd);

		assertThat(document).isNotNull();

		List<Object> boxObject = (List<Object>) document.get("$box");

		assertThat(boxObject)
				.isEqualTo((Object) Arrays.asList(GeoConverters.toList(box.getFirst()), GeoConverters.toList(box.getSecond())));
	}

	@Test // DATAMONGO-1607
	public void convertsPointCorrectlyWhenUsingNonDoubleForCoordinates() {

		assertThat(DocumentToPointConverter.INSTANCE.convert(new Document().append("x", 1L).append("y", 2L)))
				.isEqualTo(new Point(1, 2));
	}

	@Test // DATAMONGO-1607
	public void convertsCircleCorrectlyWhenUsingNonDoubleForCoordinates() {

		Document circle = new Document();
		circle.put("center", new Document().append("x", 1).append("y", 2));
		circle.put("radius", 3L);

		assertThat(DocumentToCircleConverter.INSTANCE.convert(circle))
				.isEqualTo(new Circle(new Point(1, 2), new Distance(3)));
	}

	@Test // DATAMONGO-1607
	public void convertsSphereCorrectlyWhenUsingNonDoubleForCoordinates() {

		Document sphere = new Document();
		sphere.put("center", new Document().append("x", 1).append("y", 2));
		sphere.put("radius", 3L);

		assertThat(DocumentToSphereConverter.INSTANCE.convert(sphere))
				.isEqualTo(new Sphere(new Point(1, 2), new Distance(3)));
	}

}
