/**
 * FILE: VizQueryOperator.java
 * PATH: org.datasyslab.babylon.core.vizqueryoperator.VizQueryOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizqueryoperator;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.internalobject.PixelValuePair;
import org.datasyslab.babylon.core.utils.RasterizationUtils;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * Created by jiayu on 6/22/17.
 */
public class VizQueryOperator {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(VizQueryOperator.class);


    private static JavaPairRDD<Integer, Object> spatialPartitioning(JavaRDD<Object> spatialRDD, final StandardQuadTree partitionTree) {
        return spatialRDD.flatMapToPair(
                new PairFlatMapFunction<Object, Integer, Object>() {
                    @Override
                    public Iterator<Tuple2<Integer, Object>> call(Object spatialObject) throws Exception {
                        return PartitionJudgement.getPartitionID(partitionTree, spatialObject);
                    }
                }
        ).partitionBy(new SpatialPartitioner(partitionTree.getTotalNumLeafNode()));
    }

    /**
     * Prepara join window set.
     *
     * @param spatialRDD the spatial RDD
     * @param partitionTree the partition tree
     * @param datasetBoundary the dataset boundary
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @return the java pair RDD
     */
    public static JavaPairRDD<Integer,Object> PreparaJoinWindowSet(JavaRDD<Object> spatialRDD, StandardQuadTree partitionTree, final Envelope datasetBoundary, final int resolutionX, final int resolutionY, final boolean reverseSpatialCoordinate)
    {
        // Do coordinate transformaton on polygon
        JavaRDD<Object> convertedSpatialRDD = spatialRDD.flatMap(new FlatMapFunction<Object, Object>(){
            @Override
            public Iterator<Object> call(Object spatialObject) throws Exception {
                GeometryFactory geometryFactory = new GeometryFactory();
                List<Object> result = new ArrayList<Object>();
                if(!datasetBoundary.covers(((Polygon) spatialObject).getEnvelopeInternal())) return result.iterator();
                Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,((Polygon) spatialObject).getCoordinates(),reverseSpatialCoordinate,false,true);
                try {
                    result.add(geometryFactory.createPolygon(spatialCoordinates));
                }
                catch (IllegalArgumentException e)
                {
                    return result.iterator();
                }
                return result.iterator();
            }});
        return spatialPartitioning(convertedSpatialRDD, partitionTree);

    }

    /**
     * Viz spatial join.
     *
     * @param pixelPairRDD the pixel pair RDD
     * @param spatialPartitionedRDD the spatial partitioned RDD
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> VizSpatialJoin(JavaPairRDD<Pixel, Double> pixelPairRDD, JavaPairRDD<Integer,Object> spatialPartitionedRDD)
    {
        logger.info("[Babylon][VizSpatialJoin][Start]");
        logger.debug("[Babylon][VizSpatialJoin] spatialPartitionQueryRDD has count: "+spatialPartitionedRDD.count());
        logger.debug("[Babylon][VizSpatialJoin] pixelPairRDD has count: "+pixelPairRDD.count());
        assert pixelPairRDD.getNumPartitions() == spatialPartitionedRDD.getNumPartitions();
        for (Partition p:pixelPairRDD.partitions())
        {
            logger.debug("[Babylon][VizSpatialJoin] pixelPairRDD partition " +p.toString());
        }
        for (Partition p:spatialPartitionedRDD.partitions())
        {
            logger.debug("[Babylon][VizSpatialJoin] spatialPartitionedRDD partition " +p.toString());
        }
        JavaRDD<PixelValuePair> vizJoinResult = pixelPairRDD.zipPartitions(spatialPartitionedRDD, new FlatMapFunction2<Iterator<Tuple2<Pixel,Double>>, Iterator<Tuple2<Integer,Object>>, PixelValuePair>() {
            @Override
            public Iterator<PixelValuePair> call(Iterator<Tuple2<Pixel, Double>> pixelIterator, Iterator<Tuple2<Integer, Object>> polygonIterator) throws Exception {
                List<PixelValuePair> pixelValuePairList = new ArrayList<PixelValuePair>();
                STRtree pixelTree = new STRtree();
                GeometryFactory geometryFactory = new GeometryFactory();

                while(pixelIterator.hasNext()) {
                    //Tuple2<Pixel,Double> pixelCount = pixelIterator.next();
                    //pixelTree.insert(new Envelope(pixelCount._1().getX(),pixelCount._1().getX(),pixelCount._1().getY(),pixelCount._1().getY()),pixelCount);


                    Tuple2<Pixel, Double> pixelCount = pixelIterator.next();
                    while (polygonIterator.hasNext()) {
                        Tuple2<Integer, Object> windowPair = polygonIterator.next();
                        assert pixelCount._1().getCurrentUniquePartitionId() == windowPair._1();
                        //if (((Polygon)windowPair._2()).covers(geometryFactory.createPoint(pixelCount._1())))
                        {
                            pixelValuePairList.add(new PixelValuePair(pixelCount._1(), pixelCount._2()));
                            //break;
                        }
                    }
                    //pixelValuePairList.add(new PixelValuePair(pixelCount._1(), pixelCount._2()));
                }
                return pixelValuePairList.iterator();
            }
        });
        logger.debug("[Babylon][VizSpatialJoin] vizJoinResult has count: "+vizJoinResult.count());
        logger.info("[Babylon][VizSpatialJoin][Stop]");

        return vizJoinResult.mapToPair(new PairFunction<PixelValuePair, Pixel, Double>() {
            @Override
            public Tuple2<Pixel, Double> call(PixelValuePair pixelValuePair) throws Exception {
                return new Tuple2<Pixel,Double>(pixelValuePair.pixel,pixelValuePair.value);
            }
        });
    }



}
