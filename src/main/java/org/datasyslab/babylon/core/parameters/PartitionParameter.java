/**
 * FILE: PartitionParameter.java
 * PATH: org.datasyslab.babylon.core.parameters.PartitionParameter.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.parameters;

import org.apache.spark.broadcast.Broadcast;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;

import java.io.Serializable;
import java.util.HashMap;

// TODO: Auto-generated Javadoc
/**
 * The Class PartitionParameter.
 */
public class PartitionParameter implements Serializable{
    
    /** The broadcast partition tree. */
    public Broadcast<StandardQuadTree<Integer>> broadcastPartitionTree = null;
    
    /** The broadcast partition unique id small id. */
    public Broadcast<HashMap<Integer,Integer>> broadcastPartitionUniqueIdSmallId = null;
    
    /** The total image partitions. */
    public int totalImagePartitions = 0;
    
    /**
     * Instantiates a new partition parameter.
     *
     * @param broadcastPartitionTree the broadcast partition tree
     * @param broadcastPartitionUniqueIdSmallId the broadcast partition unique id small id
     */
    public PartitionParameter(Broadcast<StandardQuadTree<Integer>> broadcastPartitionTree, Broadcast<HashMap<Integer,Integer>> broadcastPartitionUniqueIdSmallId)
    {
        this.broadcastPartitionTree = broadcastPartitionTree;
        this.broadcastPartitionUniqueIdSmallId = broadcastPartitionUniqueIdSmallId;
        totalImagePartitions = this.broadcastPartitionTree.getValue().getTotalNumLeafNode();
    }
}
