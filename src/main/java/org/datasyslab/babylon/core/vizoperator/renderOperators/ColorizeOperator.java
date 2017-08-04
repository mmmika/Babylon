/**
 * FILE: ColorizeOperator.java
 * PATH: org.datasyslab.babylon.core.vizoperator.renderOperators.ColorizeOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizoperator.renderOperators;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.babylon.core.internalobject.ColoringRule;
import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;

import java.awt.*;

// TODO: Auto-generated Javadoc
/**
 * The Class ColorizeOperator.
 */
public class ColorizeOperator {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(ColorizeOperator.class);

    /**
     * Colorize.
     *
     * @param distributedRasterCountMatrix the distributed raster count matrix
     * @param globalParameter the global parameter
     * @return the java pair RDD
     */
    public static JavaPairRDD<Pixel, Double> Colorize(JavaPairRDD<Pixel, Double> distributedRasterCountMatrix, final GlobalParameter globalParameter)
    {
        logger.info("[Babylon][Colorize][Start]");
        double tempfinalMaxPixelWeight = -1;
        if (globalParameter.maxPixelWeight>=0)
        {
            tempfinalMaxPixelWeight = globalParameter.maxPixelWeight;
        }
        else
        {
            tempfinalMaxPixelWeight = distributedRasterCountMatrix.max(new RasterPixelCountComparator())._2;
        }
        final double finalMaxPixelWeight = tempfinalMaxPixelWeight;
        logger.info("[Babylon][Colorize]maxCount is "+finalMaxPixelWeight);
        final Double minWeight = 0.0;
        JavaPairRDD<Pixel, Double> resultDistributedRasterCountMatrix = distributedRasterCountMatrix.mapValues(new Function<Double, Double>()
        {

            @Override
            public Double call(Double pixelCount) throws Exception {
                Double currentPixelCount = pixelCount;
                if(currentPixelCount > finalMaxPixelWeight)
                {
                    currentPixelCount = finalMaxPixelWeight;
                }
                Double normalizedPixelCount = (currentPixelCount-minWeight)*255/(finalMaxPixelWeight-minWeight);
                Double pixelColor = globalParameter.coloringRule.EncodeToRGB(normalizedPixelCount, globalParameter)*1.0;
                return pixelColor;
            }
        });
        logger.info("[Babylon][Colorize][Stop]");
        return resultDistributedRasterCountMatrix;
    }
}
