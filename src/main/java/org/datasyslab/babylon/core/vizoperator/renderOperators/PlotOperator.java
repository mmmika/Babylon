/**
 * FILE: PlotOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.renderOperators.PlotOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator.renderOperators;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.babylon.core.internalobject.ImageSerializableWrapper;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.internalobject.Pixel;
import scala.Tuple2;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.*;
import java.util.List;
// TODO: Auto-generated Javadoc
class RasterPixelCountComparator implements Comparator<Tuple2<Pixel, Double>>, Serializable
{
    @Override
    public int compare(Tuple2<Pixel, Double> spatialObject1, Tuple2<Pixel, Double> spatialObject2) {
        if(spatialObject1._2>spatialObject2._2)
        {
            return 1;
        }
        else if(spatialObject1._2<spatialObject2._2)
        {
            return -1;
        }
        else return 0;
    }
}

/**
 * The Class PlotOperator.
 */
public class PlotOperator {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(PlotOperator.class);

    /**
     * Plot.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Integer,ImageSerializableWrapper> Plot(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, final GlobalParameter globalParameter)
    {
        if (globalParameter.minTreeLevel!=0)
        {
            logger.info("[Babylon][PlotImageTiles][Start]");
            final double partitionIntervalX = globalParameter.resolutionX*1.0/ globalParameter.partitionsOnSingleAxis;
            final double partitionIntervalY = globalParameter.resolutionY*1.0/ globalParameter.partitionsOnSingleAxis;
            JavaPairRDD<Integer,ImageSerializableWrapper> resultDistributedRasterImage = distributedRasterCountMatrix.mapPartitionsToPair(
                    new PairFlatMapFunction<Iterator<Tuple2<Pixel,Double>>,Integer,ImageSerializableWrapper>()
                    {
                        @Override
                        public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Pixel, Double>> currentPartition)
                                throws Exception {
                            //int localPartitionIntervalX = (int)Math.ceil(partitionIntervalX);
                            //int localPartitionIntervalY = (int) Math.ceil(partitionIntervalY);
                            int localPartitionIntervalX = (int)Math.round(partitionIntervalX);
                            int localPartitionIntervalY = (int) Math.round(partitionIntervalY);
                            logger.debug("[Babylon][PlotImageTiles] rendering a data partition...partitionIntervalX "+partitionIntervalX+" partitionIntervalY "+partitionIntervalY);
                            BufferedImage imagePartition = new BufferedImage(localPartitionIntervalX,localPartitionIntervalY,BufferedImage.TYPE_INT_ARGB);
                            Tuple2<Pixel,Double> pixelColor=null;
                            while(currentPartition.hasNext())
                            {
                                //RenderOperator color in this image partition pixel-wise.
                                pixelColor = currentPartition.next();

                                if (pixelColor._1().getX()<0||pixelColor._1().getX()>= globalParameter.resolutionX||pixelColor._1().getY()<0||pixelColor._1().getY()>= globalParameter.resolutionY)
                                {
                                    pixelColor = null;
                                    continue;
                                }
                                imagePartition.setRGB(pixelColor._1().getX()%localPartitionIntervalX, (localPartitionIntervalY -1)-pixelColor._1().getY()%localPartitionIntervalY, pixelColor._2.intValue());
                            }
                            java.util.List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                            if (pixelColor==null)
                            {
                                // No pixels in this partition. Skip this subimage
                                return result.iterator();
                            }

                            result.add(new Tuple2<Integer, ImageSerializableWrapper>(pixelColor._1().getParentPartitionId(),new ImageSerializableWrapper(imagePartition)));

                            return result.iterator();
                        }
                    });
            resultDistributedRasterImage = resultDistributedRasterImage.reduceByKey(new Function2<ImageSerializableWrapper, ImageSerializableWrapper, ImageSerializableWrapper>() {
                @Override
                public ImageSerializableWrapper call(ImageSerializableWrapper image1, ImageSerializableWrapper image2) throws Exception {
                    // The combined image should be a image partition
                    BufferedImage combinedImage = new BufferedImage((int)Math.round(partitionIntervalX), (int) Math.round(partitionIntervalY), BufferedImage.TYPE_INT_ARGB);
                    Graphics graphics = combinedImage.getGraphics();
                    graphics.drawImage(image1.image, 0, 0, null);
                    graphics.drawImage(image2.image, 0, 0, null);
                    return new ImageSerializableWrapper(combinedImage);
                }
            });
            logger.info("[Babylon][PlotImageTiles][Stop]");
            return resultDistributedRasterImage;
        }
        else
        {
            logger.info("[Babylon][PlotSingleImage][Start]");
            // Draw full size image in parallel
            JavaPairRDD<Integer,ImageSerializableWrapper> resultDistributedRasterImage = distributedRasterCountMatrix.mapPartitionsToPair(
                    new PairFlatMapFunction<Iterator<Tuple2<Pixel,Double>>,Integer,ImageSerializableWrapper>()
                    {
                        @Override
                        public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Pixel, Double>> currentPartition)
                                throws Exception {
                            BufferedImage imagePartition = new BufferedImage(globalParameter.resolutionX, globalParameter.resolutionY,BufferedImage.TYPE_INT_ARGB);
                            Tuple2<Pixel,Double> pixelColor=null;
                            while(currentPartition.hasNext())
                            {
                                //RenderOperator color in this image partition pixel-wise.
                                pixelColor = currentPartition.next();
                                if (pixelColor._1().getX()<0||pixelColor._1().getX()>= globalParameter.resolutionX||pixelColor._1().getY()<0||pixelColor._1().getY()>= globalParameter.resolutionY)
                                {
                                    pixelColor = null;
                                    continue;
                                }
                                imagePartition.setRGB(pixelColor._1().getX(), (globalParameter.resolutionY - 1) -pixelColor._1().getY(), pixelColor._2.intValue());
                            }
                            List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                            if (pixelColor==null)
                            {
                                // No pixels in this partition. Skip this subimage
                                return result.iterator();
                            }
                            result.add(new Tuple2<Integer, ImageSerializableWrapper>(0,new ImageSerializableWrapper(imagePartition)));
                            return result.iterator();
                        }
                    });
            // Merge images together using reduce
            resultDistributedRasterImage = resultDistributedRasterImage.reduceByKey(new Function2<ImageSerializableWrapper, ImageSerializableWrapper, ImageSerializableWrapper>() {
                @Override
                public ImageSerializableWrapper call(ImageSerializableWrapper image1, ImageSerializableWrapper image2) throws Exception {
                    // The combined image should be a full size image
                    BufferedImage combinedImage = new BufferedImage(globalParameter.resolutionX, globalParameter.resolutionY, BufferedImage.TYPE_INT_ARGB);
                    Graphics graphics = combinedImage.getGraphics();
                    graphics.drawImage(image1.image, 0, 0, null);
                    graphics.drawImage(image2.image, 0, 0, null);
                    return new ImageSerializableWrapper(combinedImage);
                }
            });
            //logger.debug("[Babylon][RenderImage]Merged all images into one image. Result size should be 1. Actual size is "+this.distributedRasterImage.count());
            //List<Tuple2<Integer,ImageSerializableWrapper>> imageList = resultDistributedRasterImage.collect();
            logger.info("[Babylon][PlotSingleImage][Stop]");
            return resultDistributedRasterImage;
        }
    }

}
