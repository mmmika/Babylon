/**
 * FILE: RenderOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.RenderOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator;

import org.apache.spark.api.java.JavaPairRDD;
import org.datasyslab.babylon.core.internalobject.ImageSerializableWrapper;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.vizoperator.renderOperators.ColorizeOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.PhotoFilterOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.PlotOperator;

// TODO: Auto-generated Javadoc
/**
 * The Class RenderOperator.
 */
public class RenderOperator {

    /**
     * Render.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @param partitionParameter the partition parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Integer,ImageSerializableWrapper> Render(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix,
                                                                                 final GlobalParameter globalParameter, final PartitionParameter partitionParameter) {
            JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = PhotoFilterOperator.PhotoFilter(distributedRasterCountMatrix, globalParameter, partitionParameter);
            resultDistributedRasterCountMatrix = ColorizeOperator.Colorize(resultDistributedRasterCountMatrix,globalParameter);
            return PlotOperator.Plot(resultDistributedRasterCountMatrix, globalParameter);
    }
}
