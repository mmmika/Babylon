/**
 * FILE: MultiRangeViz.java
 * PATH: org.datasyslab.babylon.example.MultiRangeViz.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.example;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.GenericVizEffect;
import org.datasyslab.babylon.core.ImageGenerator;
import org.datasyslab.babylon.core.ImageStitcher;
import org.datasyslab.babylon.core.enumerator.ImageType;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.queryoperator.RangeQueryOperator;
import org.datasyslab.babylon.core.vizoperator.PixelAggregateOperator;
import org.datasyslab.babylon.core.vizoperator.PixelizeOperator;
import org.datasyslab.babylon.core.vizoperator.RenderOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.ColorizeOperator;
import org.datasyslab.babylon.core.vizoperator.renderOperators.PlotOperator;
import org.datasyslab.babylon.core.vizpartitioner.VizPartitioner;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class MultiRangeViz.
 */
public class MultiRangeViz extends GenericVizEffect {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(MultiRangeViz.class);

    /** The query windows. */
    public List<Polygon> queryWindows = new ArrayList<Polygon>();
    
    /** The output location. */
    public String outputLocation = null;
    
    /**
     * Instantiates a new multi range viz.
     *
     * @param globalParameter the global parameter
     * @param queryWindows the query windows
     * @param outputLocation the output location
     */
    public MultiRangeViz(GlobalParameter globalParameter, List<Polygon> queryWindows, String outputLocation) {
        super(globalParameter);
        this.queryWindows = queryWindows;
        this.outputLocation = outputLocation;
    }

    /* (non-Javadoc)
     * @see org.datasyslab.babylon.core.GenericVizEffect#RunOperators(org.apache.spark.api.java.JavaSparkContext, org.datasyslab.geospark.spatialRDD.SpatialRDD)
     */
    @Override
    public boolean RunOperators(JavaSparkContext sparkContext, SpatialRDD spatialRDD)
    {
        JavaPairRDD<Pixel, Double> cachedistributedRasterCountMaxtrix = PixelizeOperator.Pixelize(spatialRDD, globalParameter);
        PartitionParameter partitionParameter = VizPartitioner.initPartitionInfo(sparkContext, cachedistributedRasterCountMaxtrix, globalParameter);
        cachedistributedRasterCountMaxtrix = VizPartitioner.vizPartition(cachedistributedRasterCountMaxtrix, globalParameter, partitionParameter);
        cachedistributedRasterCountMaxtrix = PixelAggregateOperator.Aggregate(cachedistributedRasterCountMaxtrix,globalParameter);
        cachedistributedRasterCountMaxtrix = cachedistributedRasterCountMaxtrix.cache();
        for(int i=0;i<this.queryWindows.size();i++)
        {
            JavaPairRDD<Pixel, Double> distributedRasterCountMaxtrix = RangeQueryOperator.SpatialRangeQuery(cachedistributedRasterCountMaxtrix,queryWindows.get(i),globalParameter);
            distributedRasterImage = RenderOperator.Render(distributedRasterCountMaxtrix,globalParameter,partitionParameter);
            ImageGenerator imageGenerator = new ImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(distributedRasterImage,outputLocation+"window"+i, ImageType.PNG,globalParameter);
            ImageStitcher.stitchImagePartitionsFromLocalFile(outputLocation+"window"+i,globalParameter);
        }
        return true;
    }
}
