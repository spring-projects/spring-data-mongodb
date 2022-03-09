package org.springframework.data.mongodb.repository;

import com.querydsl.core.annotations.QueryEntity;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
@QueryEntity
public class Building {
    public GeoJsonPoint location;
}
