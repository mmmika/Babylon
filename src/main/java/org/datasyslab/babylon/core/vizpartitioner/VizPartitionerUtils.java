/**
 * FILE: VizPartitionerUtils.java
 * PATH: org.datasyslab.babylon.core.vizpartitioner.VizPartitionerUtils.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.vizpartitioner;

import org.datasyslab.babylon.core.internalobject.Pixel;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.parameters.PartitionParameter;
import org.datasyslab.babylon.core.internalobject.PhotoFilter;
import org.datasyslab.babylon.core.utils.RasterizationUtils;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class VizPartitionerUtils.
 */
public class VizPartitionerUtils {
    
    /**
     * Assign partition IDs to this pixel. One pixel may have more than one partition Id. This partitioning method will introduce
     * duplicates to ensure that all neighby pixels (as well as their buffer) are in the same partition.
     *
     * @param pixelDoubleTuple2 the pixel double tuple 2
     * @param photoFilter the photo filter
     * @param globalParameter the global parameter
     * @param partitionParameter the partition parameter
     * @return the list
     */
    public static List<Tuple2<Pixel, Double>> assignPartitionIDs(Tuple2<Pixel, Double> pixelDoubleTuple2, PhotoFilter photoFilter, GlobalParameter globalParameter, PartitionParameter partitionParameter)
    {
        List<Tuple2<Pixel, Double>> duplicatePixelList = new ArrayList<Tuple2<Pixel, Double>>();
        // First, calculate the correct partition that the pixel belongs to
        Tuple2<Integer,Integer> partitionId = null;
        try {
            partitionId = CalculatePartitionId(pixelDoubleTuple2._1.getX(), pixelDoubleTuple2._1.getY(), globalParameter, partitionParameter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Pixel newPixel = new Pixel(pixelDoubleTuple2._1().getX(),pixelDoubleTuple2._1().getY(), globalParameter.resolutionX, globalParameter.resolutionY,false,partitionId._1(),partitionId._2());

        duplicatePixelList.add(new Tuple2<Pixel, Double>(newPixel, pixelDoubleTuple2._2()));


        int[] boundaryCondition = {-1,0,1};
        for(int x : boundaryCondition)
        {
            for (int y:boundaryCondition)
            {
                Tuple2<Integer,Integer> duplicatePartitionId = null;
                try {
                    duplicatePartitionId = CalculatePartitionId(pixelDoubleTuple2._1().getX()+x* photoFilter.getFilterRadius(),
                            pixelDoubleTuple2._1().getY()+y* photoFilter.getFilterRadius(), globalParameter, partitionParameter);
                } catch (Exception e) {
                    // This neighbor pixel is bad. Just skip it.
                    continue;
                }
                if(duplicatePartitionId._1()!=partitionId._1()&&duplicatePartitionId._1()>=0&&duplicatePartitionId._2()>=0)
                {
                    Pixel newPixelDuplicate = new Pixel(pixelDoubleTuple2._1().getX(),pixelDoubleTuple2._1().getY(), globalParameter.resolutionX, globalParameter.resolutionY,true,duplicatePartitionId._1(),duplicatePartitionId._2());
                    duplicatePixelList.add(new Tuple2<Pixel, Double>(newPixelDuplicate, pixelDoubleTuple2._2()));
                }
            }
        }
        return duplicatePixelList;
    }

    /**
     * Calculate partition id.
     *
     * @param coordinateX the coordinate X
     * @param coordinateY the coordinate Y
     * @param globalParameter the global parameter
     * @param partitionParameter the partition parameter
     * @return the tuple 2
     * @throws Exception the exception
     */
    public static Tuple2<Integer,Integer> CalculatePartitionId( int coordinateX, int coordinateY, GlobalParameter globalParameter,
                                                               PartitionParameter partitionParameter) throws Exception {
        QuadRectangle targetPartition = partitionParameter.broadcastPartitionTree.getValue().getZone(coordinateX,coordinateY);
        QuadRectangle parentPartition = partitionParameter.broadcastPartitionTree.getValue().getParentZone(coordinateX,coordinateY, globalParameter.minTreeLevel);
        return new Tuple2<Integer, Integer>(targetPartition.partitionId,CalculateParentPartitionId(parentPartition, globalParameter));
    }

    /**
     * Calculate parent partition id.
     *
     * @param zone the zone
     * @param globalParameter the global parameter
     * @return the integer
     */
    public static Integer CalculateParentPartitionId(QuadRectangle zone, GlobalParameter globalParameter)
    {
        Long partitionCoordinateX = Math.round(zone.getEnvelope().getMinX()/ globalParameter.partitionIntervalX);
        Long partitionCoordinateY = Math.round(zone.getEnvelope().getMinY()/ globalParameter.partitionIntervalY);
        int numPartition = -1;
        try {
            numPartition = RasterizationUtils.Encode2DTo1DId(globalParameter.partitionsOnSingleAxis, globalParameter.partitionsOnSingleAxis,partitionCoordinateX.intValue(),globalParameter.partitionsOnSingleAxis-1-partitionCoordinateY.intValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return numPartition;
    }
}
