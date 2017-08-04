/**
 * FILE: PhotoFilterOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.renderOperators.PhotoFilterOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator.renderOperators;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.babylon.core.internalobject.PhotoFilter;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.vizpartitioner.VizPartitionerUtils;
import scala.Tuple2;

import java.util.*;

// TODO: Auto-generated Javadoc
/**
 * The Class PhotoFilterOperator.
 */
public class PhotoFilterOperator {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(PhotoFilterOperator.class);

    /**
     * Photo filter.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @param partitionParameter the partition parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> PhotoFilter(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, final GlobalParameter globalParameter, final PartitionParameter partitionParameter)
    {
        if (globalParameter.photoFilter.getFilterRadius()==0)
        {
            return distributedRasterCountMatrix;
        }
        if (globalParameter.minTreeLevel!=0)
        {
            logger.info("[Babylon][PhotoFilterImageTiles][Start]");
            JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel,Double>>,Pixel,Double>()
            {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> currentPartition) throws Exception  {
                    HashMap<Pixel, Double> currentPartitionHashMap = new HashMap<Pixel,Double>();
                    while (currentPartition.hasNext())
                    {
                        Tuple2<Pixel, Double> pixelDoubleTuple = currentPartition.next();
                        currentPartitionHashMap.put(pixelDoubleTuple._1(),pixelDoubleTuple._2());
                    }
                    Iterator<Map.Entry<Pixel, Double>> currentPartitionIterator = currentPartitionHashMap.entrySet().iterator();
                    // This function uses an efficient algorithm to recompute the pixel value. For each existing pixel,
                    // we calculate its impact for all pixels within its impact range and add its impact values.
                    List<Tuple2<Pixel,Double>> pixelCountList = new ArrayList<Tuple2<Pixel, Double>>();
                    while(currentPartitionIterator.hasNext())
                    {
                        Map.Entry<Pixel, Double> currentPixelCount = currentPartitionIterator.next();
                        Tuple2<Integer,Integer> centerPixelCoordinate = new Tuple2<Integer,Integer>(currentPixelCount.getKey().getX(),currentPixelCount.getKey().getY());
                        if(centerPixelCoordinate._1()<0||centerPixelCoordinate._1()>= globalParameter.resolutionX||centerPixelCoordinate._2()<0||centerPixelCoordinate._2()>= globalParameter.resolutionY)
                        {
                            // This center pixel is out of boundary so that we don't record its sum. We don't plot this pixel on the final sub image.
                            continue;
                        }
                        // Find all pixels that are in the working pixel's impact range
                        Double neighborPixelCount = 0.0;
                        if(currentPixelCount.getKey().getCurrentUniquePartitionId()<0)
                        {
                            logger.error("[VisualizationOperator][ApplyPhotoFilter] this pixel doesn't have currentPartitionId that is assigned in VisualizationPartitioner.");
                        }

                        for (int x = -globalParameter.photoFilter.getFilterRadius(); x <= globalParameter.photoFilter.getFilterRadius(); x++) {
                            for (int y = -globalParameter.photoFilter.getFilterRadius(); y <= globalParameter.photoFilter.getFilterRadius(); y++) {
                                int neighborPixelX = centerPixelCoordinate._1+x;
                                int neighborPixelY = centerPixelCoordinate._2+y;
                                if (neighborPixelX<0||neighborPixelX>= globalParameter.resolutionX||neighborPixelY<0||neighborPixelY>= globalParameter.resolutionY)
                                {
                                    // This neighbor pixel is out of boundary so that we don't record its sum. We don't plot this pixel on the final sub image.
                                    continue;
                                }

                                if(VizPartitionerUtils.CalculatePartitionId(neighborPixelX, neighborPixelY, globalParameter, partitionParameter)._1()!=currentPixelCount.getKey().getCurrentUniquePartitionId())
                                {
                                    // This neighbor pixel is not in this image partition so that we don't record its sum. We don't plot this pixel on the final sub image.
                                    continue;
                                }

                                Double tmpNeighborPixelCount = currentPartitionHashMap.get(new Pixel(neighborPixelX, neighborPixelY, globalParameter.resolutionX, globalParameter.resolutionY));
                                // For that pixel, sum up its new count
                                if (tmpNeighborPixelCount!=null)
                                {
                                    neighborPixelCount+=tmpNeighborPixelCount*globalParameter.photoFilter.getConvolutionMatrix()[x + globalParameter.photoFilter.getFilterRadius()][y + globalParameter.photoFilter.getFilterRadius()];
                                    //logger.warn("[Babylon][ApplyPhotoFilter] Got a neighbor pixel");
                                }
                                else
                                {
                                    //logger.warn("[Babylon][ApplyPhotoFilter] lose a neighbor pixel");
                                }
                            }
                        }
                        pixelCountList.add(new Tuple2<Pixel, Double>(currentPixelCount.getKey(),neighborPixelCount));
                    }
                    return pixelCountList.iterator();
                }
            },true);

            logger.info("[Babylon][PhotoFilterImageTiles][Stop]");
            return resultDistributedRasterCountMatrix;
        }
        else
        {
            logger.info("[Babylon][PhotoFilterSingleImage][Stop]");
            JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.flatMapToPair(new PairFlatMapFunction<Tuple2<Pixel, Double>, Pixel, Double>() {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Pixel, Double> pixelCount) throws Exception {
                    Tuple2<Integer,Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>(pixelCount._1().getX(),pixelCount._1().getY());
                    List<Tuple2<Pixel,Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
                    for (int x = -globalParameter.photoFilter.getFilterRadius(); x <= globalParameter.photoFilter.getFilterRadius(); x++) {
                        for (int y = -globalParameter.photoFilter.getFilterRadius(); y <= globalParameter.photoFilter.getFilterRadius(); y++) {
                            int neighborPixelX = centerPixelCoordinate._1+x;
                            int neighborPixelY = centerPixelCoordinate._2+y;
                            Double pixelCountValue = 0.0;
                            if(neighborPixelX>= globalParameter.resolutionX||neighborPixelX<0||neighborPixelY>= globalParameter.resolutionY||neighborPixelY<0)
                            {
                                continue;
                            }
                            pixelCountValue=pixelCount._2()*globalParameter.photoFilter.getConvolutionMatrix()[x + globalParameter.photoFilter.getFilterRadius()][y + globalParameter.photoFilter.getFilterRadius()];
                            result.add(new Tuple2<Pixel, Double>(new Pixel(neighborPixelX,neighborPixelY, globalParameter.resolutionX, globalParameter.resolutionY),pixelCountValue));
                        }
                    }
                    return result.iterator();
                }
            });
            resultDistributedRasterCountMatrix = resultDistributedRasterCountMatrix.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double count1, Double count2) throws Exception {
                    return count1+count2;
                }
            });
            logger.info("[Babylon][PhotoFilterSingleImage][Stop]");
            return resultDistributedRasterCountMatrix;
        }

    }

}
