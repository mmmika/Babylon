package org.datasyslab.babylon.core.queryoperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.babylon.core.internalobject.Pixel;
import scala.Tuple2;

public class JoinQueryOperator {
    public static JavaPairRDD<Pixel,Double> SpatialJoinQuery(JavaPairRDD<Pixel, Double> inputRDD, JavaRDD<Geometry> windowRDD)
    {
        JavaRDD<Tuple2<Pixel,Double>> mergeRDD = inputRDD.map(new Function<Tuple2<Pixel,Double>, Tuple2<Pixel,Double>>() {

            @Override
            public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> v1) throws Exception {
                return new Tuple2<Pixel,Double>(v1._1(),v1._2());
            }
        });
        JavaRDD<Tuple2<Pixel,Double>> tempResult = mergeRDD.zipPartitions(windowRDD,new PixelJoinGeometryJudgement());
        return tempResult.mapToPair(new PairFunction<Tuple2<Pixel, Double>, Pixel, Double>() {
            @Override
            public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> pixelDoubleTuple2) throws Exception {
                return new Tuple2<Pixel,Double>(pixelDoubleTuple2._1(),pixelDoubleTuple2._2());
            }
        });
    }

    public JavaPairRDD<Pixel,Double> SpatialJoinQuery(JavaPairRDD<Pixel, Double> inputRDD, JavaRDD<Polygon> windowRDD, double distance)
    {
        JavaRDD<Tuple2<Pixel,Double>> mergeRDD = inputRDD.map(new Function<Tuple2<Pixel,Double>, Tuple2<Pixel,Double>>() {

            @Override
            public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> v1) throws Exception {
                return new Tuple2<Pixel,Double>(v1._1(),v1._2());
            }
        });
        JavaRDD<Tuple2<Pixel,Double>> tempResult = mergeRDD.zipPartitions(windowRDD,new PixelJoinGeometryJudgement());
        return tempResult.mapToPair(new PairFunction<Tuple2<Pixel, Double>, Pixel, Double>() {
            @Override
            public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> pixelDoubleTuple2) throws Exception {
                return new Tuple2<Pixel,Double>(pixelDoubleTuple2._1(),pixelDoubleTuple2._2());
            }
        });
    }
}
