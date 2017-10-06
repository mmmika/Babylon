/**
 * FILE: ImageStitcher.java
 * PATH: org.datasyslab.babylon.core.ImageStitcher.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.datasyslab.babylon.core.internalobject.BigBufferedImage;
import org.datasyslab.babylon.core.enumerator.ImageType;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.utils.PixelizationUtils;
import org.datasyslab.babylon.core.utils.S3Operator;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

// TODO: Auto-generated Javadoc
/**
 * The Class ImageStitcher.
 */
public class ImageStitcher {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(ImageStitcher.class);

    /**
     * Stitch image partitions from local file.
     *
     * @param imageTilePath the image tile path
     * @param globalParameter the global parameter
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromLocalFile(String imageTilePath, GlobalParameter globalParameter)
    {
        logger.info("[Babylon][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(globalParameter.resolutionX, globalParameter.resolutionY,BufferedImage.TYPE_INT_ARGB);
        //Stitch all image partitions together
        for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++)
        {
            BufferedImage imageTile = null;
            try {
                imageTile = ImageIO.read(new File(""+imageTilePath+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i)+".png"));
            } catch (IOException e) {
                continue;
            }
            Tuple2<Integer,Integer> partitionCoordinate = PixelizationUtils.Decode1DTo2DId(globalParameter.partitionsOnSingleAxis, globalParameter.partitionsOnSingleAxis, i);
            int partitionMinX = partitionCoordinate._1*Math.round(globalParameter.resolutionX/globalParameter.partitionsOnSingleAxis);
            int partitionMinY = partitionCoordinate._2*Math.round(globalParameter.resolutionY/globalParameter.partitionsOnSingleAxis);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX+imageTile.getWidth();
            int partitionMaxY = partitionMinY+imageTile.getHeight();
            logger.debug("[Babylon][stitchImagePartitions] stitching image tile..."+i+" ResolutionX " + globalParameter.resolutionX+" ResolutionY "+globalParameter.resolutionY);
            logger.debug("[Babylon][stitchImagePartitions] stitching a image tile..."+i+" MinX "+partitionMinX+" MaxX "+partitionMaxX+" MinY "+partitionMinY+" MaxY "+partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(stitchedImage,imageTilePath+"-"+globalParameter.minTreeLevel+"-stitched", ImageType.PNG);
        logger.info("[Babylon][stitchImagePartitions][Stop]");
        return true;
    }

    /**
     * Stitch image partitions from S 3 file.
     *
     * @param regionName the region name
     * @param accessKey the access key
     * @param secretKey the secret key
     * @param bucketName the bucket name
     * @param imageTilePath the image tile path
     * @param globalParameter the global parameter
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromS3File(String regionName, String accessKey, String secretKey, String bucketName, String imageTilePath, GlobalParameter globalParameter)
    {
        logger.info("[Babylon][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(globalParameter.resolutionX, globalParameter.resolutionY,BufferedImage.TYPE_INT_ARGB);
        S3Operator s3Operator = new S3Operator(regionName, accessKey, secretKey);
        //Stitch all image partitions together
        for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++)
        {
            BufferedImage imageTile = null;
            try {
                imageTile = s3Operator.getImage(bucketName, imageTilePath+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i)+".png");
            } catch (AmazonS3Exception e) {
                continue;
            }
            Tuple2<Integer,Integer> partitionCoordinate = PixelizationUtils.Decode1DTo2DId(globalParameter.partitionsOnSingleAxis, globalParameter.partitionsOnSingleAxis, i);
            int partitionMinX = partitionCoordinate._1*Math.round(globalParameter.resolutionX/globalParameter.partitionsOnSingleAxis);
            int partitionMinY = partitionCoordinate._2*Math.round(globalParameter.resolutionY/globalParameter.partitionsOnSingleAxis);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX+imageTile.getWidth();
            int partitionMaxY = partitionMinY+imageTile.getHeight();
            logger.debug("[Babylon][stitchImagePartitions] stitching image tile..."+i+" ResolutionX " + globalParameter.resolutionX+" ResolutionY "+globalParameter.resolutionY);
            logger.debug("[Babylon][stitchImagePartitions] stitching a image tile..."+i+" MinX "+partitionMinX+" MaxX "+partitionMaxX+" MinY "+partitionMinY+" MaxY "+partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsS3File(stitchedImage,regionName, accessKey, secretKey, bucketName,imageTilePath+"-"+globalParameter.minTreeLevel+"-stitched", ImageType.PNG);
        logger.info("[Babylon][stitchImagePartitions][Stop]");
        return true;
    }

    /**
     * Stitch image partitions from hadoop file.
     *
     * @param imageTilePath the image tile path
     * @param globalParameter the global parameter
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromHadoopFile(String imageTilePath, GlobalParameter globalParameter)
    {
        logger.info("[Babylon][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(globalParameter.resolutionX, globalParameter.resolutionY,BufferedImage.TYPE_INT_ARGB);

        String[] splitString = imageTilePath.split(":");
        String hostName = splitString[0]+":"+splitString[1];
        String[] portAndPath = splitString[2].split("/");
        String port = portAndPath[0];
        String localPath = "";
        for(int i=1;i<portAndPath.length;i++)
        {
            localPath+="/"+portAndPath[i];
        }

        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new URI(hostName+":"+port), hadoopConf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        //Stitch all image partitions together
        for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++)
        {
            BufferedImage imageTile = null;
            try {
                if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i)+".png")))
                {
                    InputStream inputStream = hdfs.open(new org.apache.hadoop.fs.Path(localPath+"-"+i+".png"));
                    imageTile = ImageIO.read(inputStream);
                    inputStream.close();
                    hdfs.close();
                }
                else
                {
                    continue;
                }
            } catch (IOException e) {
                continue;
            }
            Tuple2<Integer,Integer> partitionCoordinate = PixelizationUtils.Decode1DTo2DId(globalParameter.partitionsOnSingleAxis, globalParameter.partitionsOnSingleAxis, i);
            int partitionMinX = partitionCoordinate._1*Math.round(globalParameter.resolutionX/globalParameter.partitionsOnSingleAxis);
            int partitionMinY = partitionCoordinate._2*Math.round(globalParameter.resolutionY/globalParameter.partitionsOnSingleAxis);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX+imageTile.getWidth();
            int partitionMaxY = partitionMinY+imageTile.getHeight();
            logger.debug("[Babylon][stitchImagePartitions] stitching image tile..."+i+" ResolutionX " + globalParameter.resolutionX+" ResolutionY "+globalParameter.resolutionY);
            logger.debug("[Babylon][stitchImagePartitions] stitching a image tile..."+i+" MinX "+partitionMinX+" MaxX "+partitionMaxX+" MinY "+partitionMinY+" MaxY "+partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(stitchedImage,imageTilePath+"-"+globalParameter.minTreeLevel+"-stitched", ImageType.PNG);
        logger.info("[Babylon][stitchImagePartitions][Stop]");
        return true;
    }
}
