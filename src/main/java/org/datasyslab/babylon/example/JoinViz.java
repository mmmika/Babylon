/**
 * FILE: JoinViz.java
 * PATH: org.datasyslab.babylon.example.JoinViz.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.example;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.GenericVizEffect;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.queryoperator.JoinQueryOperator;
import org.datasyslab.babylon.core.queryoperator.RangeQueryOperator;
import org.datasyslab.babylon.core.vizoperator.PixelAggregateOperator;
import org.datasyslab.babylon.core.vizoperator.PixelizeOperator;
import org.datasyslab.babylon.core.vizoperator.RenderOperator;
import org.datasyslab.babylon.core.vizpartitioner.VizPartitioner;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

// TODO: Auto-generated Javadoc
/**
 * The Class JoinViz.
 */
public class JoinViz extends GenericVizEffect {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(JoinViz.class);

    /** The window RDD. */
    public SpatialRDD windowRDD;

    /**
     * Instantiates a new join viz.
     *
     * @param globalParameter the global parameter
     * @param windowRDD the window RDD
     */
    public JoinViz(GlobalParameter globalParameter, SpatialRDD windowRDD) {
        super(globalParameter);
        this.windowRDD = windowRDD;
    }

    /* (non-Javadoc)
     * @see org.datasyslab.babylon.core.GenericVizEffect#RunOperators(org.apache.spark.api.java.JavaSparkContext, org.datasyslab.geospark.spatialRDD.SpatialRDD)
     */
    @Override
    public boolean RunOperators(JavaSparkContext sparkContext, SpatialRDD spatialRDD)
    {
        JavaPairRDD<Pixel, Double> distributedRasterCountMaxtrix = PixelizeOperator.Pixelize(spatialRDD, globalParameter);

        // Partition SpatialRDD
        PartitionParameter partitionParameter = VizPartitioner.initPartitionInfo(sparkContext, distributedRasterCountMaxtrix, globalParameter);
        distributedRasterCountMaxtrix = VizPartitioner.vizPartition(distributedRasterCountMaxtrix, globalParameter, partitionParameter);
        distributedRasterCountMaxtrix = PixelAggregateOperator.Aggregate(distributedRasterCountMaxtrix,globalParameter);

        // Partition WindowRDD
        JavaRDD<Geometry> partitionedWindowRDD = VizPartitioner.vizPartition(windowRDD,globalParameter,partitionParameter);

        // Partition do join query. Polygon windows join pixel data
        distributedRasterCountMaxtrix = JoinQueryOperator.SpatialJoinQuery(distributedRasterCountMaxtrix,partitionedWindowRDD);
        distributedRasterImage = RenderOperator.Render(distributedRasterCountMaxtrix,globalParameter,partitionParameter);
        return true;
    }
}
