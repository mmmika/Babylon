/**
 * FILE: PixelizeOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.PixelizeOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.utils.PixelizationUtils;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.util.Iterator;

// TODO: Auto-generated Javadoc
/**
 * The Class PixelizeOperator.
 */
public class PixelizeOperator {
    
    /**
     * Pixelize.
     *
     * @param spatialRDD the spatial RDD
     * @param globalParameter the global parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> Pixelize(SpatialRDD spatialRDD, final GlobalParameter globalParameter)
    {
        return Pixelize(spatialRDD.rawSpatialRDD,globalParameter);
    }

    public static JavaPairRDD<Pixel, Double> Pixelize(JavaRDD<Geometry> spatialRDD, final GlobalParameter globalParameter)
    {
        JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = spatialRDD.flatMapToPair(new PairFlatMapFunction<Geometry,Pixel,Double>()
        {
            @Override
            public Iterator<Tuple2<Pixel, Double>> call(Geometry spatialObject) throws Exception {

                if (globalParameter.drawOutlineOnly)
                {
                    if(spatialObject instanceof Point)
                    {
                        return PixelizationUtils.FindOutlinePixelCoordinates(globalParameter.resolutionX, globalParameter.resolutionY,globalParameter.datasetBoundary,(Point)spatialObject, globalParameter.pixelAggregatorOption, globalParameter.reverseSpatialCoordinate).iterator();
                    }
                    else if(spatialObject instanceof Polygon)
                    {
                        return PixelizationUtils.FindOutlinePixelCoordinates(globalParameter.resolutionX, globalParameter.resolutionY,globalParameter.datasetBoundary,(Polygon)spatialObject,globalParameter.pixelAggregatorOption, globalParameter.reverseSpatialCoordinate).iterator();
                    }
                    else if(spatialObject instanceof LineString)
                    {
                        return PixelizationUtils.FindOutlinePixelCoordinates(globalParameter.resolutionX, globalParameter.resolutionY,globalParameter.datasetBoundary,(LineString)spatialObject,globalParameter.pixelAggregatorOption, globalParameter.reverseSpatialCoordinate).iterator();
                    }
                    else
                    {
                        throw new Exception("[Babylon][Rasterize] Babylon rasterize draw outline only strategy only supports Point, Polygon, LineString");
                    }
                }
                else
                {
                    if(spatialObject instanceof Polygon)
                    {
                        return PixelizationUtils.FindFillingAreaPixelCoordinates(globalParameter.resolutionX, globalParameter.resolutionY,globalParameter.datasetBoundary,(Polygon)spatialObject,(double)((Polygon) spatialObject).getUserData(), globalParameter.reverseSpatialCoordinate).iterator();
                    }
                    else
                    {
                        throw new Exception("[Babylon][Rasterize] Babylon rasterize filling area strategy only supports Polygon");
                    }
                }

            }
        });
        spatialRDDwithPixelId = spatialRDDwithPixelId.filter(new Function<Tuple2<Pixel, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Pixel, Double> pixelCount) throws Exception {
                if (pixelCount._1().getX() < 0 || pixelCount._1().getX() >= globalParameter.resolutionX || pixelCount._1().getY() < 0 || pixelCount._1().getY() >= globalParameter.resolutionY)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        });
        return spatialRDDwithPixelId;
    }

}
