/**
 * FILE: ImageGenerator.java
 * PATH: org.datasyslab.babylon.core.ImageGenerator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.image.BufferedImage;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.babylon.core.enumerator.ImageType;

import org.datasyslab.babylon.core.internalobject.ImageSerializableWrapper;
import org.datasyslab.babylon.core.parameters.GlobalParameter;
import org.datasyslab.babylon.core.utils.PixelizationUtils;
import org.datasyslab.babylon.core.utils.S3Operator;
import scala.Tuple2;

import javax.imageio.ImageIO;


// TODO: Auto-generated Javadoc
/**
 * The Class ImageGenerator.
 */
public class ImageGenerator implements Serializable{

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(ImageGenerator.class);
	/**
	 * Save raster image as spark file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 * @deprecated
	 */
	public boolean SaveRasterImageAsSparkFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveRasterImageAsSparkFile][Start]");

        // Locate HDFS path
        String[] splitString = outputPath.split(":");
        String hostName = splitString[0]+":"+splitString[1];
        String[] portAndPath = splitString[2].split("/");
        String port = portAndPath[0];
        String localPath = "";
        for(int i=1;i<portAndPath.length;i++)
        {
            localPath+="/"+portAndPath[i];
        }
        localPath+="."+imageType.getTypeName();
        // Delete existing files
        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        logger.info("[Babylon][SaveRasterImageAsSparkFile] HDFS URI BASE: "+hostName+":"+port);
        FileSystem hdfs = FileSystem.get(new URI(hostName+":"+port), hadoopConf);
        logger.info("[Babylon][SaveRasterImageAsSparkFile] Check the existence of path: "+localPath);
        if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath))) {
            logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleting path: "+localPath);
            hdfs.delete(new org.apache.hadoop.fs.Path(localPath), true);
            logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleted path: "+localPath);
        }

		distributedImage.saveAsObjectFile(outputPath+"."+imageType.getTypeName());
		logger.info("[Babylon][SaveRasterImageAsSparkFile][Stop]");
		return true;
	}
	
	/**
	 * Save raster image as local file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @param globalParameter the global parameter
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsLocalFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage, final String outputPath, final ImageType imageType, GlobalParameter globalParameter)
	{
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Start]");
		if (globalParameter.overwriteExistingImages)
		{
			for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++) {
				deleteLocalFile(outputPath+"-"+PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i),imageType);
			}
		}

		distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>() {
			@Override
			public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2) throws Exception {
				SaveRasterImageAsLocalFile(integerImageSerializableWrapperTuple2._2.image, outputPath+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,integerImageSerializableWrapperTuple2._1), imageType);
			}
		});
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Stop]");
		return true;
	}


	/**
	 * Save raster image as hadoop file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @param globalParameter the global parameter
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsHadoopFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage, final String outputPath, final ImageType imageType, GlobalParameter globalParameter)
	{
		logger.info("[Babylon][SaveRasterImageAsHadoopFile][Start]");

		if (globalParameter.overwriteExistingImages)
		{
			for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++) {
				deleteHadoopFile(outputPath+"-"+PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i)+".", imageType);
			}
		}
		distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>() {
			@Override
			public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2) throws Exception {
				SaveRasterImageAsHadoopFile(integerImageSerializableWrapperTuple2._2.image, outputPath+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,integerImageSerializableWrapperTuple2._1), imageType);
			}
		});
		logger.info("[Babylon][SaveRasterImageAsHadoopFile][Stop]");
		return true;
	}

	/**
	 * Save raster image as S 3 file.
	 *
	 * @param distributedImage the distributed image
	 * @param regionName the region name
	 * @param accessKey the access key
	 * @param secretKey the secret key
	 * @param bucketName the bucket name
	 * @param path the path
	 * @param imageType the image type
	 * @param globalParameter the global parameter
	 * @return true, if successful
	 */
	public boolean SaveRasterImageAsS3File(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage,
											   final String regionName, final String accessKey, final String secretKey,
										   final String bucketName, final String path, final ImageType imageType, GlobalParameter globalParameter)
	{
		logger.info("[Babylon][SaveRasterImageAsS3File][Start]");
		S3Operator s3Operator = new S3Operator(regionName, accessKey, secretKey);
		if (globalParameter.overwriteExistingImages)
		{
			for(int i=0;i<globalParameter.partitionsOnSingleAxis*globalParameter.partitionsOnSingleAxis;i++) {
				s3Operator.deleteImage(bucketName, path+"-"+PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,i)+"."+imageType.getTypeName());
			}
		}
		distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>() {
			@Override
			public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2) throws Exception {
				SaveRasterImageAsS3File(integerImageSerializableWrapperTuple2._2.image, regionName, accessKey, secretKey, bucketName, path+"-"+ PixelizationUtils.getImageTileName(globalParameter.minTreeLevel,globalParameter.partitionsOnSingleAxis,integerImageSerializableWrapperTuple2._1), imageType);
			}
		});
		logger.info("[Babylon][SaveRasterImageAsS3File][Stop]");
		return true;
	}

	/**
	 * Save raster image as local file.
	 *
	 * @param rasterImage the raster image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsLocalFile(BufferedImage rasterImage, String outputPath, ImageType imageType)
	{
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Start]");
		File outputImage = new File(outputPath+"."+imageType.getTypeName());
		outputImage.getParentFile().mkdirs();
		try {
			ImageIO.write(rasterImage,imageType.getTypeName(),outputImage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Stop]");
		return true;
	}

	/**
	 * Save raster image as hadoop file.
	 *
	 * @param rasterImage the raster image
	 * @param originalOutputPath the original output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsHadoopFile(BufferedImage rasterImage, String originalOutputPath, ImageType imageType)
	{
		logger.info("[Babylon][SaveRasterImageAsHadoopFile][Start]");
		// Locate HDFS path
		String outputPath = originalOutputPath+"."+imageType.getTypeName();
		String[] splitString = outputPath.split(":");
		String hostName = splitString[0]+":"+splitString[1];
		String[] portAndPath = splitString[2].split("/");
		String port = portAndPath[0];
		String localPath = "";
		for(int i=1;i<portAndPath.length;i++)
		{
			localPath+="/"+portAndPath[i];
		}
		localPath+="."+imageType.getTypeName();
		// Delete existing files
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		logger.info("[Babylon][SaveRasterImageAsSparkFile] HDFS URI BASE: "+hostName+":"+port);
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(new URI(hostName+":"+port), hadoopConf);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		logger.info("[Babylon][SaveRasterImageAsSparkFile] Check the existence of path: "+localPath);
		try {
			if (hdfs.exists(new Path(localPath))) {
                logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleting path: "+localPath);
                hdfs.delete(new Path(localPath), true);
                logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleted path: "+localPath);
            }
		} catch (IOException e) {
			e.printStackTrace();
		}
		Path path = new Path(outputPath);
		FSDataOutputStream out = null;
		try {
			out = hdfs.create(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			ImageIO.write(rasterImage,"png",out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("[Babylon][SaveRasterImageAsHadoopFile][Stop]");
		return true;
	}



	/**
	 * Save raster image as S 3 file.
	 *
	 * @param rasterImage the raster image
	 * @param regionName the region name
	 * @param accessKey the access key
	 * @param secretKey the secret key
	 * @param bucketName the bucket name
	 * @param path the path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean SaveRasterImageAsS3File(BufferedImage rasterImage, String regionName, String accessKey, String secretKey, String bucketName, String path, ImageType imageType) {
		logger.info("[Babylon][SaveRasterImageAsS3File][Start]");
		S3Operator s3Operator = new S3Operator(regionName,accessKey, secretKey);
		s3Operator.putImage(bucketName,path+"."+imageType.getTypeName(),rasterImage);
		logger.info("[Babylon][SaveRasterImageAsS3File][Stop]");
		return true;
	}

	/**
	 * Save vector image as spark file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveVectorImageAsSparkFile(JavaPairRDD<Integer,String> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveVectorImageAsSparkFile][Start]");

		// Locate HDFS path
		String[] splitString = outputPath.split(":");
		String hostName = splitString[0]+":"+splitString[1];
		String[] portAndPath = splitString[2].split("/");
		String port = portAndPath[0];
		String localPath = "";
		for(int i=1;i<portAndPath.length;i++)
		{
			localPath+="/"+portAndPath[i];
		}
        localPath+="."+imageType.getTypeName();
        // Delete existing files
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		logger.info("[Babylon][SaveVectorImageAsSparkFile] HDFS URI BASE: "+hostName+":"+port);
		FileSystem hdfs = FileSystem.get(new URI(hostName+":"+port), hadoopConf);
        logger.info("[Babylon][SaveVectorImageAsSparkFile] Check the existence of path: "+localPath);
        if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath))) {
            logger.info("[Babylon][SaveVectorImageAsSparkFile] Deleting path: "+localPath);
            hdfs.delete(new org.apache.hadoop.fs.Path(localPath), true);
            logger.info("[Babylon][SaveVectorImageAsSparkFile] Deleted path: "+localPath);
        }

		distributedImage.saveAsTextFile(outputPath+"."+imageType.getTypeName());
		logger.info("[Babylon][SaveVectorImageAsSparkFile][Stop]");

		return true;
	}

	/**
	 * Save vectormage as local file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveVectormageAsLocalFile(JavaPairRDD<Integer,String> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveVectormageAsLocalFile][Start]");
		JavaRDD<String> distributedVectorImageNoKey= distributedImage.map(new Function<Tuple2<Integer,String>, String>()
		{

			@Override
			public String call(Tuple2<Integer, String> vectorObject) throws Exception {
				return vectorObject._2();
			}

		});
		this.SaveVectorImageAsLocalFile(distributedVectorImageNoKey.collect(), outputPath, imageType);
		logger.info("[Babylon][SaveVectormageAsLocalFile][Stop]");
		return true;
	}

	/**
	 * Save vector image as local file.
	 *
	 * @param vectorImage the vector image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveVectorImageAsLocalFile(List<String> vectorImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveVectorImageAsLocalFile][Start]");
		File outputImage = new File(outputPath+"."+imageType.getTypeName());
		outputImage.getParentFile().mkdirs();

		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter(outputImage);
			bw = new BufferedWriter(fw);
			for(String svgElement : vectorImage)
			{
				bw.write(svgElement);
			}

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
		logger.info("[Babylon][SaveVectorImageAsLocalFile][Stop]");
		return true;
	}

	/**
	 * Delete hadoop file.
	 *
	 * @param originalOutputPath the original output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean deleteHadoopFile(String originalOutputPath, ImageType imageType) {
		String outputPath = originalOutputPath+"."+imageType.getTypeName();
		String[] splitString = outputPath.split(":");
		String hostName = splitString[0]+":"+splitString[1];
		String[] portAndPath = splitString[2].split("/");
		String port = portAndPath[0];
		String localPath = "";
		for(int i=1;i<portAndPath.length;i++)
		{
			localPath+="/"+portAndPath[i];
		}
		localPath+="."+imageType.getTypeName();
		// Delete existing files
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		logger.info("[Babylon][SaveRasterImageAsSparkFile] HDFS URI BASE: "+hostName+":"+port);
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(new URI(hostName+":"+port), hadoopConf);
			logger.info("[Babylon][SaveRasterImageAsSparkFile] Check the existence of path: "+localPath);
			if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath))) {
				logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleting path: "+localPath);
				hdfs.delete(new org.apache.hadoop.fs.Path(localPath), true);
				logger.info("[Babylon][SaveRasterImageAsSparkFile] Deleted path: "+localPath);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * Delete local file.
	 *
	 * @param originalOutputPath the original output path
	 * @param imageType the image type
	 * @return true, if successful
	 */
	public boolean deleteLocalFile(String originalOutputPath, ImageType imageType) {
		File file = null;
		try {

			// create new file
			file = new File(originalOutputPath+"."+imageType.getTypeName());

			// tries to delete a non-existing file
			file.delete();

		} catch(Exception e) {

			// if any error occurs
			e.printStackTrace();
		}
		return true;
	}


}
