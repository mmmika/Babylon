/**
 * FILE: VizPartitioner.java
 * PATH: org.datasyslab.babylon.core.vizpartitioner.VizPartitioner.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizpartitioner;

import java.io.Serializable;
import java.util.*;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.random.SamplingUtils;
import org.datasyslab.babylon.core.internalobject.PhotoFilter;
import org.datasyslab.babylon.core.internalobject.Pixel;

import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.utils.Logarithm;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import static org.datasyslab.babylon.core.utils.PixelizationUtils.ConvertToPixelCoordinate;

// TODO: Auto-generated Javadoc
/**
 * The Class VizPartitioner.
 */
public class VizPartitioner extends Partitioner implements Serializable{

	private int resolutionX,resolutionY;
	private int partitionsOnSingleAxis;
	private double partitionIntervalX, partitionIntervalY;
	private int totalImagePartitions = -1;

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(VizPartitioner.class);

	/**
	 * Instantiates a new visualization partitioner.
	 *
	 * @param globalParameter the global parameter
	 * @param partitionParameter the partition parameter
	 */
	public VizPartitioner(GlobalParameter globalParameter, PartitionParameter partitionParameter)
	{
		this.resolutionX = globalParameter.resolutionX;
		this.resolutionY = globalParameter.resolutionY;
		this.totalImagePartitions = partitionParameter.totalImagePartitions;
		this.partitionIntervalX = globalParameter.partitionIntervalX;
		this.partitionIntervalY = globalParameter.partitionIntervalY;
		this.partitionsOnSingleAxis = globalParameter.partitionsOnSingleAxis;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
	 */
	@Override
	public int getPartition(Object key) {
		if (key instanceof Pixel)
		{
			return ((Pixel) key).getCurrentUniquePartitionId();
		}
		else return (int)key;	}

	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#numPartitions()
	 */
	@Override
	public int numPartitions() {
		return totalImagePartitions;
	}

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
		StandardQuadTree.maxLevel = globalParameter.maxPartitionTreeLevel<(new Long(Math.round(Logarithm.log(Math.min(globalParameter.resolutionX, globalParameter.resolutionY)/ PhotoFilter.MIN_PARTITION_INTERVAL,2))).intValue())
				?globalParameter.maxPartitionTreeLevel:(new Long(Math.round(Logarithm.log(Math.min(globalParameter.resolutionX, globalParameter.resolutionY)/ PhotoFilter.MIN_PARTITION_INTERVAL,2))).intValue());
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

		logger.info("[Babylon][initPartitionInfo] sampling fraction is "+ globalParameter.samplingFraction);

		List<Tuple2<Pixel,Double>> sampleList = distributedRasterCountMatrix.sample(false,globalParameter.samplingFraction).collect();

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
					VizPartitioner vizPartitioner = new VizPartitioner(globalParameter, partitionParameter);
					List<Tuple2<Pixel,Double>> result = new ArrayList<Tuple2<Pixel,Double>>();
					while (tuple2Iterator.hasNext())
					{
						result.addAll(VizPartitionerUtils.assignPartitionIDs(tuple2Iterator.next(), globalParameter.photoFilter, globalParameter, partitionParameter));
					}
					return result.iterator();
				}
			});
			resultDistributedRasterCountMatrix = resultDistributedRasterCountMatrix.partitionBy(new VizPartitioner(globalParameter, partitionParameter));
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
			resultDistributedRasterCountMatrix = resultDistributedRasterCountMatrix.partitionBy(new VizPartitioner(globalParameter, partitionParameter));
			logger.info("[Babylon][vizPartitionNoBuffer][Stop]");
			return  resultDistributedRasterCountMatrix;
		}

	}
	
	/**
	 * Viz partition.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param globalParameter the global parameter
	 * @param partitionParameter the partition parameter
	 * @return the java RDD
	 */
	public static JavaRDD<Geometry> vizPartition(SpatialRDD spatialRDD, final GlobalParameter globalParameter, final PartitionParameter partitionParameter)
	{
		return vizPartition(spatialRDD.rawSpatialRDD,globalParameter,partitionParameter);
	}

	/**
	 * Viz partition.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param globalParameter the global parameter
	 * @param partitionParameter the partition parameter
	 * @return the java RDD
	 */
	public static JavaRDD<Geometry> vizPartition(JavaRDD<Geometry> spatialRDD, final GlobalParameter globalParameter, final PartitionParameter partitionParameter)
	{
		logger.info("[Babylon][vizPartitionSpatialRDD][Start]");
		JavaPairRDD<Integer, Geometry> resultSpatialRDD = spatialRDD.flatMapToPair(new PairFlatMapFunction<Geometry, Integer, Geometry>() {
			@Override
			public Iterator<Tuple2<Integer, Geometry>> call(Geometry geometry) throws Exception {
				Set<Tuple2<Integer, Geometry>> result = new HashSet<>();
				boolean containFlag = false;
				ArrayList<QuadRectangle> matchedPartitions = new ArrayList<QuadRectangle>();
				Geometry convertedSpatialObject= ConvertToPixelCoordinate(globalParameter.resolutionX,globalParameter.resolutionY,globalParameter.datasetBoundary,geometry,globalParameter.reverseSpatialCoordinate);
				try {
					partitionParameter.broadcastPartitionTree.getValue().getZone(matchedPartitions,
							new QuadRectangle(convertedSpatialObject.getEnvelopeInternal()));
				}
				catch (NullPointerException e)
				{
					return result.iterator();
				}
				for(int i=0;i<matchedPartitions.size();i++)
				{
					containFlag=true;
					result.add(new Tuple2(matchedPartitions.get(i).partitionId, convertedSpatialObject));
				}

				if (containFlag == false) {
					//throw new Exception("This object cannot find partition: " +spatialObject);

					// This object is not covered by the partition. Should be dropped.
					// Partition tree from StandardQuadTree do not have missed objects.
				}
				return result.iterator();
			}
		});
		resultSpatialRDD = resultSpatialRDD.partitionBy(new VizPartitioner(globalParameter, partitionParameter));
		JavaRDD<Geometry> mergedResultRDD = resultSpatialRDD.map(new Function<Tuple2<Integer, Geometry>, Geometry>() {
			@Override
			public Geometry call(Tuple2<Integer, Geometry> v1) throws Exception {
				return v1._2();
			}
		});
		logger.info("[Babylon][vizPartitionSpatialRDD][Stop]");
		return  mergedResultRDD;
	}
}
