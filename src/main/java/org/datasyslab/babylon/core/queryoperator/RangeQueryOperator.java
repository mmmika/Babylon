/**
 * FILE: RangeQueryOperator.java
 * PATH: org.datasyslab.babylon.core.queryoperator.RangeQueryOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.queryoperator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import static org.datasyslab.babylon.core.utils.PixelizationUtils.ConvertToPixelCoordinate;

// TODO: Auto-generated Javadoc
/**
 * The Class RangeQueryOperator.
 */
public class RangeQueryOperator {
    
    /**
     * Spatial range query.
     *
     * @param inputRDD the input RDD
     * @param queryWindow the query window
     * @return the java RDD
     */
    public static JavaRDD<Geometry> SpatialRangeQuery(SpatialRDD inputRDD, Polygon queryWindow)
    {
        return inputRDD.rawSpatialRDD.filter(new Function<Geometry, Boolean>() {
            @Override
            public Boolean call(Geometry v1) throws Exception {
                return queryWindow.contains(v1);
            }
        });
    }
    
    /**
     * Spatial range query.
     *
     * @param inputRDD the input RDD
     * @param queryWindow the query window
     * @param globalParameter the global parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> SpatialRangeQuery(JavaPairRDD<Pixel, Double> inputRDD, Polygon queryWindow, GlobalParameter globalParameter)
    {
        return inputRDD.filter(new Function<Tuple2<Pixel, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Pixel, Double> v1) throws Exception {
                GeometryFactory geometryFactory = new GeometryFactory();
                //return true;
                return ConvertToPixelCoordinate(globalParameter.resolutionX,globalParameter.resolutionY,globalParameter.datasetBoundary,queryWindow,globalParameter.reverseSpatialCoordinate).contains(geometryFactory.createPoint(new Coordinate(v1._1().getX(),v1._1.getY())));
            }
        });
    }
}
