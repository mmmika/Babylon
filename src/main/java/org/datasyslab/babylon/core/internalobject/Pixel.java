/**
 * FILE: Pixel.java
 * PATH: org.datasyslab.babylon.core.internalobject.Pixel.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.internalobject;

import com.vividsolutions.jts.geom.Coordinate;
import org.datasyslab.babylon.core.utils.RasterizationUtils;
import scala.Tuple2;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class Pixel.
 */
public class Pixel extends Coordinate implements Serializable{
    
    /** The x. */
    private int x;
    
    /** The y. */
    private int y;
    
    /** The resolution X. */
    private int resolutionX;
    
    /** The resolution Y. */
    private int resolutionY;
    
    /** The is duplicate. */
    private boolean isDuplicate = false;

    // currentUniquePartitionId can be equal/different to parentPartitionId

    private int currentUniquePartitionId = -1;

    private Integer parentPartitionId = -1;

    /**
     * Instantiates a new pixel.
     *
     * @param x the x
     * @param y the y
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param isDuplicate the is duplicate
     * @param currentUniquePartitionId the current unique partition id
     * @param parentPartitionId the parent partition id
     */
    public Pixel(int x, int y, int resolutionX, int resolutionY, boolean isDuplicate, int currentUniquePartitionId, Integer parentPartitionId)
    {
        super(x,y);
        this.x = x;
        this.y = y;
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
        this.isDuplicate = isDuplicate;
        this.currentUniquePartitionId = currentUniquePartitionId;
        this.parentPartitionId = parentPartitionId;
    }

    /**
     * Instantiates a new pixel.
     *
     * @param x the x
     * @param y the y
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     */
    public Pixel(int x, int y, int resolutionX, int resolutionY)
    {
        this.x = x;
        this.y = y;
        this.resolutionX=resolutionX;
        this.resolutionY=resolutionY;
    }
    
    /**
     * Checks if is duplicate.
     *
     * @return true, if is duplicate
     */
    public boolean isDuplicate() {
        return isDuplicate;
    }

    /**
     * Sets the duplicate.
     *
     * @param duplicate the new duplicate
     */
    public void setDuplicate(boolean duplicate) {
        isDuplicate = duplicate;
    }

    /**
     * Gets the current unique partition id.
     *
     * @return the current unique partition id
     */
    public int getCurrentUniquePartitionId() {
        return this.currentUniquePartitionId;
    }

    /**
     * Gets the parent partition id.
     *
     * @return the parent partition id
     */
    public Integer getParentPartitionId(){ return  this.parentPartitionId;}

    /**
     * Sets the current partition id.
     *
     * @param currentPartitionId the current partition id
     */
    public void setCurrentPartitionId(Tuple2<Integer,Integer> currentPartitionId) {
        this.currentUniquePartitionId = currentPartitionId._1();
        this.parentPartitionId = currentPartitionId._2();
    }

    /**
     * Gets the x.
     *
     * @return the x
     */
    public int getX() {
        return x;
    }

    /**
     * Gets the y.
     *
     * @return the y
     */
    public int getY() {
        return y;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        /*
        Pixel anotherObject = (Pixel) o;
        if(this.hashCode()==anotherObject.hashCode()&&this.getCurrentPartitionId()==anotherObject.getCurrentPartitionId()&&this.isDuplicate()==anotherObject.isDuplicate())
        {
            return true;
        }
        else return false;
        */
        return this.hashCode()==o.hashCode();
    }



    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        /*
        int result = 17;
        result = 31 * result + this.getX();
        result = 31 * result + this.getY();
        //result = 31 * result + this.getCurrentPartitionId();
        return result;
        */
        int id = -1;
        try {
            id = RasterizationUtils.Encode2DTo1DId(resolutionX,resolutionY,x,y);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

}
