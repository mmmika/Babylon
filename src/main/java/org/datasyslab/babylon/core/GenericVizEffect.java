/**
 * FILE: GenericVizEffect.java
 * PATH: org.datasyslab.babylon.core.GenericVizEffect.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.internalobject.ImageSerializableWrapper;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.vizoperator.PixelizeOperator;
import org.datasyslab.babylon.core.vizoperator.PixelAggregateOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.ColorizeOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.PhotoFilterOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.PlotOperator;
import org.datasyslab.babylon.core.vizpartitioner.VizPartitioner;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

// TODO: Auto-generated Javadoc
/**
 * The Class GenericVizEffect.
 */
public class GenericVizEffect {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(GenericVizEffect.class);

    /** The global parameter. */
    protected GlobalParameter globalParameter;
    
    /** The distributed raster image. */
    protected JavaPairRDD<Integer, ImageSerializableWrapper> distributedRasterImage;

    /**
     * Instantiates a new generic viz effect.
     *
     * @param globalParameter the global parameter
     */
    public GenericVizEffect(GlobalParameter globalParameter)
    {
        this.globalParameter = globalParameter;
    }
    
    /**
     * Sets the parameter.
     *
     * @return true, if successful
     */
    public boolean SetParameter()
    {
        return true;
    }

    /**
     * Gets the distributed raster image.
     *
     * @return the distributed raster image
     */
    public JavaPairRDD<Integer, ImageSerializableWrapper> getDistributedRasterImage()
    {
        return this.distributedRasterImage;
    }

    /**
     * Run operators.
     *
     * @param sparkContext the spark context
     * @param spatialRDD the spatial RDD
     * @return true, if successful
     */
    public boolean RunOperators(JavaSparkContext sparkContext, SpatialRDD spatialRDD)
    {
        JavaPairRDD<Pixel, Double> distributedRasterCountMaxtrix = PixelizeOperator.Pixelize(spatialRDD, globalParameter);
        PartitionParameter partitionParameter = VizPartitioner.initPartitionInfo(sparkContext, distributedRasterCountMaxtrix, globalParameter);
        distributedRasterCountMaxtrix = VizPartitioner.vizPartition(distributedRasterCountMaxtrix, globalParameter, partitionParameter);
        distributedRasterCountMaxtrix = PixelAggregateOperator.Aggregate(distributedRasterCountMaxtrix,globalParameter);
        distributedRasterCountMaxtrix = PhotoFilterOperator.PhotoFilter(distributedRasterCountMaxtrix, globalParameter, partitionParameter);
        distributedRasterCountMaxtrix = ColorizeOperator.Colorize(distributedRasterCountMaxtrix,globalParameter);
        distributedRasterImage = PlotOperator.Plot(distributedRasterCountMaxtrix,globalParameter);
        return true;
    }

    /**
     * Visualize.
     *
     * @param sparkContext the spark context
     * @param spatialRDD the spatial RDD
     * @return true, if successful
     */
    public boolean Visualize(JavaSparkContext sparkContext, SpatialRDD spatialRDD) {
        SetParameter();
        logger.info(globalParameter.toString());
        RunOperators(sparkContext, spatialRDD);
        return true;
    }

}
