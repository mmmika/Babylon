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

public class JoinViz extends GenericVizEffect {
    final static Logger logger = Logger.getLogger(JoinViz.class);

    public SpatialRDD windowRDD;

    public JoinViz(GlobalParameter globalParameter, SpatialRDD windowRDD) {
        super(globalParameter);
        this.windowRDD = windowRDD;
    }

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
