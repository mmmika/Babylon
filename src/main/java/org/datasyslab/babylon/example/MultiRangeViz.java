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

public class MultiRangeViz extends GenericVizEffect {
    final static Logger logger = Logger.getLogger(MultiRangeViz.class);

    public List<Polygon> queryWindows = new ArrayList<Polygon>();
    public String outputLocation = null;
    public MultiRangeViz(GlobalParameter globalParameter, List<Polygon> queryWindows, String outputLocation) {
        super(globalParameter);
        this.queryWindows = queryWindows;
        this.outputLocation = outputLocation;
    }

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
