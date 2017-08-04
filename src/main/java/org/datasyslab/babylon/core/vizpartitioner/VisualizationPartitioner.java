/**
 * FILE: VisualizationPartitioner.java
 * PATH: org.datasyslab.babylon.core.vizpartitioner.VisualizationPartitioner.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizpartitioner;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.datasyslab.babylon.core.internalobject.Pixel;

import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;

// TODO: Auto-generated Javadoc
/**
 * The Class VisualizationPartitioner.
 */
public class VisualizationPartitioner extends Partitioner implements Serializable{

	private int resolutionX,resolutionY;
	private int partitionsOnSingleAxis;
	private double partitionIntervalX, partitionIntervalY;
	private int totalImagePartitions = -1;

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(VisualizationPartitioner.class);

	/**
	 * Instantiates a new visualization partitioner.
	 *
	 * @param globalParameter the global parameter
	 * @param partitionParameter the partition parameter
	 */
	public VisualizationPartitioner(GlobalParameter globalParameter, PartitionParameter partitionParameter)
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
		return ((Pixel) key).getCurrentUniquePartitionId();
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#numPartitions()
	 */
	@Override
	public int numPartitions() {
		return totalImagePartitions;
	}



}
