// scalastyle:off
/*
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on
package org.apache.spark.sql.execution.datasources.v2.s3.store

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.apache.commons.csv._
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String


object S3StoreFactory{
  def getS3Store(schema: StructType,
            params: java.util.Map[String, String],
            filters: Array[Filter]): S3Store = {

    var format = params.get("format")
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new S3StoreCSV(schema, params, filters)
      case "json" => new S3StoreJSON(schema, params, filters)
      case "parquet" => new S3StoreParquet(schema, params, filters)
    }
  }
}
abstract class S3Store(schema: StructType,
                       params: java.util.Map[String, String],
                       filters: Array[Filter]) {

  protected var path = params.get("path")
  protected val logger = LoggerFactory.getLogger(getClass)
  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }
  logger.trace("S3Store Created")

  protected val s3Credential = new BasicAWSCredentials(params.get("accessKey"),
                                                     params.get("secretKey"))
  protected val s3Client = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                               params.get("endpoint"), Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(staticCredentialsProvider(s3Credential))
    .build()

  def getRows(): List[InternalRow];
}

class S3StoreCSV(schema: StructType,
                 params: java.util.Map[String, String],
                 filters: Array[Filter]) extends S3Store(schema, params, filters) {

  override def toString() : String = "S3StoreCSV" + params + filters.mkString(", ")

  override def getRows(): List[InternalRow] = {
    var records = new ListBuffer[InternalRow]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    val csvFormat = CSVFormat.DEFAULT
      .withHeader(schema.fields.map(x => x.name): _*)
      .withRecordSeparator("\n")
      .withDelimiter(params.getOrElse("delimiter", ",").charAt(0))
      .withQuote(params.getOrElse("quote", "\"").charAt(0))
      .withEscape(params.getOrElse(s"escape", "\\").charAt(0))
      .withCommentMarker(params.getOrElse(s"comment", "#").charAt(0))

    do {
      result = s3Client.listObjectsV2(req)
      result.getObjectSummaries().asScala.foreach(objectSummary => {
        val in = s3Client.selectObjectContent(
          Select.requestCSV(
            objectSummary.getBucketName(),
            objectSummary.getKey(),
            params,
            schema,
            filters)
        ).getPayload().getRecordsInputStream()
        var parser = CSVParser.parse(in, java.nio.charset.Charset.forName("UTF-8"), csvFormat)
        try {
          for (record <- parser.asScala) {
            records += InternalRow.fromSeq(schema.fields.map(x => {
              TypeCast.castTo(record.get(x.name), x.dataType, x.nullable)
            }))
          }
        } catch {
          case NonFatal(e) => logger.error(s"Exception while parsing ", e)
        }
        parser.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records.toList
  }
  logger.trace("S3StoreCSV: schema " + schema)
  logger.trace("S3StoreCSV: path " + params.get("path"))
  logger.trace("S3StoreCSV: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreCSV: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreCSV: filters: " + filters.mkString(", "))
}

class S3StoreJSON(schema: StructType,
                  params: java.util.Map[String, String],
                  filters: Array[Filter]) extends S3Store(schema, params, filters) {

  override def toString() : String = "S3StoreJSON" + params + filters.mkString(", ")

  override def getRows(): List[InternalRow] = {
    var records = new ListBuffer[InternalRow]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    do {
      result = s3Client.listObjectsV2(req)
      asScalaBuffer(result.getObjectSummaries()).foreach(objectSummary => {
        val br = new BufferedReader(new InputStreamReader(
          s3Client.selectObjectContent(
            Select.requestJSON(
              objectSummary.getBucketName(),
              objectSummary.getKey(),
              params,
              schema,
              filters)
          ).getPayload().getRecordsInputStream()))
        var line : String = null
        while ( {line = br.readLine(); line != null}) {
          var row = new Array[Any](schema.fields.length)
          var rowValues = line.split(",")
          var index = 0
          while (index < rowValues.length) {
            val field = schema.fields(index)
            row(index) = TypeCast.castTo(rowValues(index), field.dataType,
              field.nullable)
            index += 1
          }
          records += InternalRow.fromSeq(row)
        }
        br.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records.toList
  }
  logger.trace("S3StoreJSON: schema " + schema)
  logger.trace("S3StoreJSON: path " + params.get("path"))
  logger.trace("S3StoreJSON: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreJSON: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreJSON: filters: " + filters.mkString(", "))
}

class S3StoreParquet(schema: StructType,
                     params: java.util.Map[String, String],
                     filters: Array[Filter]) extends S3Store(schema, params, filters) {

  override def toString() : String = "S3StoreParquet" + params + filters.mkString(", ")

  override def getRows(): List[InternalRow] = {
    var records = new ListBuffer[InternalRow]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = S3URI.toAmazonS3URI(path)
    var params: Map[String, String] = Map("" -> "")

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    do {
      result = s3Client.listObjectsV2(req)
      asScalaBuffer(result.getObjectSummaries()).foreach(objectSummary => {
        val br = new BufferedReader(new InputStreamReader(
          s3Client.selectObjectContent(
            Select.requestParquet(
              objectSummary.getBucketName(),
              objectSummary.getKey(),
              params,
              schema,
              filters)
          ).getPayload().getRecordsInputStream()))
        var line : String = null
        while ( {line = br.readLine(); line != null}) {
          var row = new Array[Any](schema.fields.length)
          var rowValues = line.split(",")
          var index = 0
          while (index < rowValues.length) {
            val field = schema.fields(index)
            row(index) = TypeCast.castTo(rowValues(index), field.dataType,
              field.nullable)
            index += 1
          }
          records += InternalRow.fromSeq(row)
        }
        br.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records.toList
  }
  logger.trace("S3StoreParquet: schema " + schema)
  logger.trace("S3StoreParquet: path " + params.get("path"))
  logger.trace("S3StoreParquet: endpoint " + params.get("endpoint"))
  logger.trace("S3StoreParquet: accessKey/secretKey " +
              params.get("accessKey") + "/" + params.get("secretKey"))
  logger.trace("S3StoreParquet: filters: " + filters.mkString(", "))
}


