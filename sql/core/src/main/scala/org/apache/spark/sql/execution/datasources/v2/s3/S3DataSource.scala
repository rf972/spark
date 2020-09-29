/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2.s3

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer}

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.v2.s3.store.{S3Store, S3StoreFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class DefaultSource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("S3 Data Source Created")
  override def toString: String = s"S3DataSource()"
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        params: util.Map[String, String]): Table = {
    logger.info("getTable: Options " + params)
    new S3BatchTable(schema, params)
  }

  override def keyPrefix(): String = {
    "s3"
  }
  override def shortName(): String = "s3datasource"
}

class S3BatchTable(schema: StructType,
                     params: util.Map[String, String])
  extends Table with SupportsRead {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("S3BatchTable Created")
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new S3ScanBuilder(schema, params)
}

class S3ScanBuilder(schema: StructType,
                      params: util.Map[String, String])
  extends ScanBuilder with SupportsPushDownFilters {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("S3ScanBuilder Created")

  var scanFilters: Array[Filter] = Array[Filter]()

  override def build(): Scan = new S3SimpleScan(schema, params, scanFilters)

  def pushedFilters: Array[Filter] = {
    logger.info("S3ScanBuilder:pushedFilters" + scanFilters.toList)
    scanFilters
  }

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.info("S3ScanBuilder:pushFilters" + filters.toList)
    scanFilters = filters
    scanFilters
  }
}

class S3SimpleScan(schema: StructType,
                     params: util.Map[String, String],
                     filters: Array[Filter])
      extends Scan with Batch{

  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("S3SimpleScan Created")
  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new S3Partition())
  }
  override def createReaderFactory(): PartitionReaderFactory =
          new S3PartitionReaderFactory(schema, params, filters)
}

class S3Partition extends InputPartition

class S3PartitionReaderFactory(schema: StructType,
                                 params: util.Map[String, String],
                                 filters: Array[Filter])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("S3PartitionReaderFactory Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
                 new S3PartitionReader(schema, params, filters)
}

class S3PartitionReader(schema: StructType,
                          params: util.Map[String, String],
                          filters: Array[Filter])
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  logger.info("S3PartitionReader Created")

  private def getStaticRows(schema: StructType, filters: Array[Filter]):
                            Seq[InternalRow] = {
    var records = new ListBuffer[InternalRow]
    val names = Array("P0", "P1", "P2", "P3", "P4")
    var name = ""; var id = 0; var age = 42;
    for { (name, id) <- (names zip (0 until names.length))} {
      records += InternalRow(id, UTF8String.fromString(name), age, UTF8String.fromString("City 0"))
    }
    records.toList
  }

  /* We pull in the entire data set as a list.
   * Then we return the data one row as a time as requested
   * Through the iterator interface.
   */
  private var store: S3Store = S3StoreFactory.getS3Store(schema, params, filters)
  private var rows = store.getRows()
  logger.info("S3PartitionReader: store " + store)
  logger.info("S3PartitionReader: rows " + rows.mkString(", "))
  logger.info("S3PartitionReader: schema " + schema)
  logger.info("S3PartitionReader: params " + params)
  logger.info("S3PartitionReader: filters: " + filters.mkString(", "))

  var index = 0
  def next: Boolean = index < rows.length

  def get: InternalRow = {
    val row = rows(index)
    logger.info("S3PartitionReader.get " + row)
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
