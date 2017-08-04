/**
 * FILE: VizPartitionOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.VizPartitionOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.internalobject.PhotoFilter;
import org.datasyslab.babylon.core.utils.Logarithm;
import org.datasyslab.babylon.core.vizpartitioner.VisualizationPartitioner;
import org.datasyslab.babylon.core.vizpartitioner.VizPartitionerUtils;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import scala.Tuple2;

import java.util.*;

// TODO: Auto-generated Javadoc
/**
 * The Class VizPartitionOperator.
 */
public class VizPartitionOperator {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(VizPartitionOperator.class);

    /**
     * Inits the partition info.
     *
     * @param sparkContext the spark context
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @return the partition parameter
     */
    public static PartitionParameter initPartitionInfo(JavaSparkContext sparkContext, JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, GlobalParameter globalParameter)
    {
        logger.info("[Babylon][initPartitionInfo][Start]");
        StandardQuadTree.maxLevel = new Long(Math.round(Logarithm.log(Math.min(globalParameter.resolutionX, globalParameter.resolutionY)/ PhotoFilter.MIN_PARTITION_INTERVAL,2))).intValue();
        StandardQuadTree.maxItemByNode = 2;
        int finalMinTreeLevel = globalParameter.minTreeLevel;
        if(globalParameter.minTreeLevel> StandardQuadTree.maxLevel)
        {
            logger.warn("[Babylon][Constructor] the min tree level "+ globalParameter.minTreeLevel+" is larger than Max Level: "+ StandardQuadTree.maxLevel+" Please increase image resolution or decrease the min tree level.");
            finalMinTreeLevel = StandardQuadTree.maxLevel;
        }
        else
        {
            finalMinTreeLevel = globalParameter.minTreeLevel;
        }

        Long sampleAmount = new Long(globalParameter.sampleAmount);

        if (sampleAmount<Math.pow(4,finalMinTreeLevel))
        {
            // Make sure don't crash on small test datasets because the average SamplePartitionVolume should be no less than 1.
            sampleAmount = (long) Math.pow(4,finalMinTreeLevel);
        }
        logger.info("[Babylon][initPartitionInfo] sample amount is "+ sampleAmount);
        List<Tuple2<Pixel,Double>> sampleList = distributedRasterCountMatrix.take(sampleAmount.intValue());

        logger.info("[Babylon][initPartitionInfo] max tree level is "+ StandardQuadTree.maxLevel);

        QuadRectangle quadtreeDefinition = new QuadRectangle(0,0, globalParameter.resolutionX, globalParameter.resolutionY);

        StandardQuadTree<Integer> completeQuadtreePartitions = new StandardQuadTree<Integer>(quadtreeDefinition,0);
        completeQuadtreePartitions.forceGrowUp(finalMinTreeLevel);

        for (int i=0;i<sampleList.size();i++)
        {
            completeQuadtreePartitions.insert(new QuadRectangle(sampleList.get(i)._1().getX(),sampleList.get(i)._1().getY(),1,1),1);
        }

        HashSet<Integer> allPartitionId = new HashSet<Integer>();
        completeQuadtreePartitions.getAllLeafNodeUniqueId(allPartitionId);
        assert allPartitionId.size() == completeQuadtreePartitions.getTotalNumLeafNode();

        HashMap<Integer, Integer> uniqueIdSmallId = new HashMap<Integer,Integer>();
        int paritionId = 0;
        Iterator<Integer> hashSetIterator = allPartitionId.iterator();
        while(hashSetIterator.hasNext())
        {
            uniqueIdSmallId.put(hashSetIterator.next(), paritionId);
            paritionId++;
        }
        completeQuadtreePartitions.decidePartitionSerialId(uniqueIdSmallId);

        PartitionParameter partitionParameter = new PartitionParameter(sparkContext.broadcast(completeQuadtreePartitions), sparkContext.broadcast(uniqueIdSmallId));
        logger.info("[Babylon][initPartitionInfo] create "+ completeQuadtreePartitions.getTotalNumLeafNode() +" partitions");
        logger.info("[Babylon][initPartitionInfo] create "+ Math.pow(4,finalMinTreeLevel) +" image tiles");
        logger.info("[Babylon][initPartitionInfo][Stop]");
        return partitionParameter;

    }
    
    /**
     * Viz partition.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @param partitionParameter the partition parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> vizPartition(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, final GlobalParameter globalParameter, final PartitionParameter partitionParameter)
    {
        if (globalParameter.filterRadius>0)
        {
            logger.info("[Babylon][vizPartitionWithBuffer][Start]");
            JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel, Double>>, Pixel, Double>() {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> tuple2Iterator) throws Exception {
                    VisualizationPartitioner vizPartitioner = new VisualizationPartitioner(globalParameter, partitionParameter);
                    List<Tuple2<Pixel,Double>> result = new ArrayList<Tuple2<Pixel,Double>>();
                    while (tuple2Iterator.hasNext())
                    {
                        result.addAll(VizPartitionerUtils.assignPartitionIDs(tuple2Iterator.next(), globalParameter.photoFilter, globalParameter, partitionParameter));
                    }
                    return result.iterator();
                }
            });
            resultDistributedRasterCountMatrix = resultDistributedRasterCountMatrix.partitionBy(new VisualizationPartitioner(globalParameter, partitionParameter));
            logger.info("[Babylon][vizPartitionWithBuffer][Stop]");
            return  resultDistributedRasterCountMatrix;
        }
        else
        {
            logger.info("[Babylon][vizPartitionNoBuffer][Start]");
            JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel, Double>>, Pixel, Double>() {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> tuple2Iterator) throws Exception {
                    List<Tuple2<Pixel,Double>> result = new ArrayList<Tuple2<Pixel,Double>>();
                    while (tuple2Iterator.hasNext())
                    {
                        Tuple2<Pixel,Double> curPixelCount = tuple2Iterator.next();
                        Pixel newPixel = new Pixel(curPixelCount._1().getX(),curPixelCount._1().getY(), globalParameter.resolutionX, globalParameter.resolutionY);
                        newPixel.setDuplicate(false);
                        newPixel.setCurrentPartitionId(VizPartitionerUtils.CalculatePartitionId(curPixelCount._1.getX(), curPixelCount._1.getY(), globalParameter, partitionParameter));
                        //assert newPixel.getParentPartitionId()>=0 && newPixel.getCurrentUniquePartitionId()>=0;
                        assert newPixel.getCurrentUniquePartitionId()>=0;
                        Tuple2<Pixel,Double> newPixelCount= new Tuple2<Pixel, Double>(newPixel, curPixelCount._2());
                        result.add(newPixelCount);
                    }
                    return result.iterator();
                }
            });
            resultDistributedRasterCountMatrix = resultDistributedRasterCountMatrix.partitionBy(new VisualizationPartitioner(globalParameter, partitionParameter));
            logger.info("[Babylon][vizPartitionNoBuffer][Stop]");
            return  resultDistributedRasterCountMatrix;
        }

    }

}
