/**
 * FILE: PixelAggregateOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.PixelAggregateOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.babylon.core.enumerator.PixelAggregatorOption;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import scala.Tuple2;

import java.util.*;

// TODO: Auto-generated Javadoc
/**
 * The Class PixelAggregateOperator.
 */
public class PixelAggregateOperator {

    /**
     * Aggregate.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> Aggregate(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, final GlobalParameter globalParameter)
    {
        JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel, Double>>, Pixel, Double>() {
            @Override
            public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> pixelWeightIterator) throws Exception {
                HashMap<Pixel, Double> counterPerPixel = new HashMap<Pixel, Double>();
                HashMap<Pixel, Double> aggregatorPerPixel = new HashMap<Pixel, Double>();

                while (pixelWeightIterator.hasNext())
                {
                    // Counter the occurrence times per pixel
                    Tuple2<Pixel, Double> pixelWeight = pixelWeightIterator.next();
                    Double currentCounter = counterPerPixel.get(pixelWeight._1());
                    if (currentCounter == null)
                    {
                        //First time to find this pixel
                        currentCounter = 1.0;
                    }
                    else
                    {
                        currentCounter++;
                    }
                    counterPerPixel.put(pixelWeight._1(),currentCounter);

                    // Aggregate the weight per pixel
                    Double currentAggregator = aggregatorPerPixel.get(pixelWeight._1());
                    if (currentAggregator == null)
                    {
                        //First time to find this pixel
                        currentAggregator = pixelWeight._2();
                    }
                    else
                    {
                        if(globalParameter.pixelAggregatorOption == PixelAggregatorOption.MAX)
                        {
                            currentAggregator = pixelWeight._2()>currentAggregator?pixelWeight._2():currentAggregator;
                        }
                        else if(globalParameter.pixelAggregatorOption == PixelAggregatorOption.MIN)
                        {
                            currentAggregator = pixelWeight._2()<currentAggregator?pixelWeight._2():currentAggregator;
                        }
                        else if(globalParameter.pixelAggregatorOption == PixelAggregatorOption.SUM)
                        {
                            currentAggregator+=pixelWeight._2();
                        }
                        else if (globalParameter.pixelAggregatorOption == PixelAggregatorOption.UNIFORM)
                        {
                            currentAggregator = 1.0;
                        }
                        else if (globalParameter.pixelAggregatorOption == PixelAggregatorOption.COUNT)
                        {
                            currentAggregator = currentCounter;
                        }
                    }
                    aggregatorPerPixel.put(pixelWeight._1(),currentAggregator);
                }
                // Convert the result to standard format
                List<Tuple2<Pixel,Double>> resultPixelWeight = new ArrayList<Tuple2<Pixel, Double>>();
                Iterator<Map.Entry<Pixel,Double>> aggregatorIterator= aggregatorPerPixel.entrySet().iterator();
                while (aggregatorIterator.hasNext())
                {
                    Map.Entry<Pixel,Double> aggregator = aggregatorIterator.next();
                    resultPixelWeight.add(new Tuple2<Pixel, Double>(aggregator.getKey(), aggregator.getValue()));
                }

                return resultPixelWeight.iterator();
            }
        }, true);
        return resultDistributedRasterCountMatrix;
    }
}
