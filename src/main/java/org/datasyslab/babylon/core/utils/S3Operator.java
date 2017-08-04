/**
 * FILE: S3Operator.java
 * PATH: org.datasyslab.babylon.core.utils.S3Operator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core.utils;

import java.awt.image.BufferedImage;
import java.io.*;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;

import javax.imageio.ImageIO;

// TODO: Auto-generated Javadoc
/**
 * The Class S3Operator.
 */
public class S3Operator {

	private AmazonS3 s3client;
	
	/** The Constant logger. */
	public final static Logger logger = Logger.getLogger(S3Operator.class);

	/**
	 * Instantiates a new s 3 operator.
	 *
	 * @param regionName the region name
	 * @param accessKey the access key
	 * @param secretKey the secret key
	 */
	public S3Operator(String regionName, String accessKey, String secretKey)
	{
		Regions region = Regions.fromName(regionName);
	    BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
		s3client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
        logger.debug("[Babylon][Constructor] Initialized a S3 client");
    }
	
	/**
	 * Creates the bucket.
	 *
	 * @param bucketName the bucket name
	 * @return true, if successful
	 */
	public boolean createBucket(String bucketName) {
		Bucket bucket = s3client.createBucket(bucketName);
        logger.debug("[Babylon][createBucket] Created a bucket: " + bucket.toString());
		return true;
	}
	
	/**
	 * Delete image.
	 *
	 * @param bucketName the bucket name
	 * @param path the path
	 * @return true, if successful
	 */
	public boolean deleteImage(String bucketName, String path) {
		s3client.deleteObject(bucketName, path);
        logger.debug("[Babylon][deleteImage] Deleted an image if exist");
        return true;
	}
	
	/**
	 * Put image.
	 *
	 * @param bucketName the bucket name
	 * @param path the path
	 * @param rasterImage the raster image
	 * @return true, if successful
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean putImage(String bucketName, String path, BufferedImage rasterImage) throws IOException {
        deleteImage(bucketName,path);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(rasterImage, "png", outputStream);
        byte[] buffer = outputStream.toByteArray();
        InputStream inputStream = new ByteArrayInputStream(buffer);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(buffer.length);
		s3client.putObject(new PutObjectRequest(bucketName, path, inputStream, metadata));
		inputStream.close();
		outputStream.close();
        logger.debug("[Babylon][putImage] Put an image");
        return true;
	}
	
	/**
	 * Gets the image.
	 *
	 * @param bucketName the bucket name
	 * @param path the path
	 * @return the image
	 * @throws Exception the exception
	 */
	public BufferedImage getImage(String bucketName, String path) throws Exception {
		logger.debug("[Babylon][getImage] Start");
		S3Object s3Object =  s3client.getObject(bucketName, path);
        InputStream inputStream = s3Object.getObjectContent();
		BufferedImage rasterImage = ImageIO.read(inputStream);
		inputStream.close();
		s3Object.close();
        logger.debug("[Babylon][getImage] Got an image");
		return rasterImage;
	}
}
