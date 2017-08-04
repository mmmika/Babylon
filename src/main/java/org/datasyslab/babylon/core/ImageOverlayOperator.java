/**
 * FILE: ImageOverlayOperator.java
 * PATH: org.datasyslab.babylon.core.ImageOverlayOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.babylon.core.internalobject.ImageSerializableWrapper;
import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class ImageOverlayOperator.
 */
public class ImageOverlayOperator {
	
	/** The back raster image. */
	public BufferedImage backRasterImage=null;
	
	/** The distributed back raster image. */
	public JavaPairRDD<Integer,ImageSerializableWrapper> distributedBackRasterImage=null;

	/** The generate distributed image. */
	public boolean generateDistributedImage=false;

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(ImageOverlayOperator.class);

	/**
	 * Instantiates a new raster overlay operator.
	 *
	 * @param backRasterImage the back raster image
	 */
	public ImageOverlayOperator(BufferedImage backRasterImage)
	{
		this.backRasterImage = backRasterImage;
		this.generateDistributedImage = false;
	}

	/**
	 * Instantiates a new raster overlay operator.
	 *
	 * @param distributedBackRasterImage the distributed back raster image
	 */
	public ImageOverlayOperator(JavaPairRDD<Integer,ImageSerializableWrapper> distributedBackRasterImage)
	{
		this.distributedBackRasterImage = distributedBackRasterImage;
		this.generateDistributedImage=true;
	}

	/**
	 * Join image.
	 *
	 * @param distributedFontImage the distributed font image
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean JoinImage(JavaPairRDD<Integer,ImageSerializableWrapper> distributedFontImage) throws Exception
	{
		logger.info("[Babylon][JoinImage][Start]");
		if (this.generateDistributedImage==false)
		{
			throw new Exception("[OverlayOperator][JoinImage] The back image is not distributed. Please don't use distributed format.");
		}
		this.distributedBackRasterImage = this.distributedBackRasterImage.cogroup(distributedFontImage).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Iterable<ImageSerializableWrapper>,Iterable<ImageSerializableWrapper>>>,Integer,ImageSerializableWrapper>()
		{
			@Override
			public Tuple2<Integer, ImageSerializableWrapper> call(
					Tuple2<Integer, Tuple2<Iterable<ImageSerializableWrapper>, Iterable<ImageSerializableWrapper>>> imagePair)
					throws Exception {
		int imagePartitionId = imagePair._1;
		Iterator<ImageSerializableWrapper> backImageIterator = imagePair._2._1.iterator();
		Iterator<ImageSerializableWrapper> frontImageIterator = imagePair._2._2.iterator();
		if(backImageIterator.hasNext()==false)
		{
			throw new Exception("[OverlayOperator][JoinImage] The back image iterator didn't get any image partitions.");
		}
		if(frontImageIterator.hasNext()==false)
		{
			throw new Exception("[OverlayOperator][JoinImage] The front image iterator didn't get any image partitions.");
		}
		BufferedImage backImage = backImageIterator.next().image;
		BufferedImage frontImage = frontImageIterator.next().image;
		if(backImage.getWidth()!=frontImage.getWidth()||backImage.getHeight()!=frontImage.getHeight())
		{
			throw new Exception("[OverlayOperator][JoinImage] The two given image don't have the same width or the same height.");
		}
		int w = Math.max(backImage.getWidth(), frontImage.getWidth());
		int h = Math.max(backImage.getHeight(), frontImage.getHeight());
		BufferedImage combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		Graphics graphics = combinedImage.getGraphics();
		graphics.drawImage(backImage, 0, 0, null);
		graphics.drawImage(frontImage, 0, 0, null);
		logger.info("[Babylon][JoinImage][Stop]");
		return new Tuple2<Integer, ImageSerializableWrapper>(imagePartitionId,new ImageSerializableWrapper(combinedImage));
			}
		});
		return true;
	}

	/**
	 * Join image.
	 *
	 * @param frontRasterImage the front raster image
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean JoinImage(BufferedImage frontRasterImage) throws Exception
	{
		if (this.generateDistributedImage==true)
		{
			throw new Exception("[OverlayOperator][JoinImage] The back image is distributed. Please don't use centralized format.");
		}
		if(backRasterImage.getWidth()!=frontRasterImage.getWidth()||backRasterImage.getHeight()!=frontRasterImage.getHeight())
		{
			throw new Exception("[OverlayOperator][JoinImage] The two given image don't have the same width or the same height.");
		}
		int w = Math.max(backRasterImage.getWidth(), frontRasterImage.getWidth());
		int h = Math.max(backRasterImage.getHeight(), frontRasterImage.getHeight());
		BufferedImage combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		Graphics graphics = combinedImage.getGraphics();
		graphics.drawImage(backRasterImage, 0, 0, null);
		graphics.drawImage(frontRasterImage, 0, 0, null);
		this.backRasterImage = combinedImage;
		return true;
	}
}
