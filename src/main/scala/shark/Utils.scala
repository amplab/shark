/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import java.io.BufferedReader
import java.util.{Map => JMap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}


object Utils {

  /**
   * Convert a memory quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def memoryBytesToString(size: Long): String = {
    lazy val TB = 1L << 40
    lazy val GB = 1L << 30
    lazy val MB = 1L << 20
    lazy val KB = 1L << 10

    def fmt(value:Double, unit:String) = "%.1f %s".formatLocal(java.util.Locale.US, value, unit)

    size match {
      case _:Long if size >= 2*TB => fmt(size.asInstanceOf[Double] / TB, "TB")
      case _:Long if size >= 2*GB => fmt(size.asInstanceOf[Double] / GB, "GB")
      case _:Long if size >= 2*MB => fmt(size.asInstanceOf[Double] / MB, "MB")
      case _:Long if size >= 2*KB => fmt(size.asInstanceOf[Double] / KB, "KB")
    }
  }

  /**
   * Set the AWS (e.g. EC2/S3) credentials from environmental variables
   * AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
   */
  def setAwsCredentials(conf: Configuration, envs: JMap[String, String] = System.getenv()) {
    if (envs.get("AWS_ACCESS_KEY_ID") != null && envs.get("AWS_SECRET_ACCESS_KEY") != null) {
      conf.set("fs.s3n.awsAccessKeyId", envs.get("AWS_ACCESS_KEY_ID"))
      conf.set("fs.s3.awsAccessKeyId", envs.get("AWS_ACCESS_KEY_ID"))
      conf.set("fs.s3n.awsSecretAccessKey", envs.get("AWS_SECRET_ACCESS_KEY"))
      conf.set("fs.s3.awsSecretAccessKey", envs.get("AWS_SECRET_ACCESS_KEY"))
    }
  }

  def isS3File(filename: String): Boolean = {
    filename.startsWith("s3n://") || filename.startsWith("s3://")
  }

  def createReaderForS3(s3path: String, conf: Configuration): BufferedReader = {

    import java.io.InputStreamReader
    import java.net.URI
    import org.jets3t.service.impl.rest.httpclient.RestS3Service
    import org.jets3t.service.security.AWSCredentials

    // Replace the s3 or s3n protocol with http so we can parse it with Java's URI class.
    val url = new URI(s3path.replaceFirst("^s3n://", "http://").replaceFirst("^s3://", "http://"))

    // Set AWS credentials
    var accessKey: String = null
    var secretKey: String = null
    if (url.getUserInfo() != null) {
      val credentials = url.getUserInfo().split("[:]")
      accessKey = credentials(0)
      secretKey = credentials(1)
    } else if (conf.get("fs.s3.awsAccessKeyId") != null &&
      conf.get("fs.s3.awsSecretAccessKey") != null) {
      accessKey = conf.get("fs.s3.awsAccessKeyId")
      secretKey = conf.get("fs.s3.awsSecretAccessKey")
    }

    // Remove the / prefix in object name.
    val objectName: String = url.getPath().substring(1)
    val bucketName: String = url.getHost()

    val s3Service = new RestS3Service(new AWSCredentials(accessKey, secretKey))
    val bucket = s3Service.getBucket(bucketName)
    val s3obj = s3Service.getObject(bucket, objectName)
    new BufferedReader(new InputStreamReader(s3obj.getDataInputStream()))
  }

  /**
   * Returns a filter that accepts files not present in the captured snapshot of the `path`
   * directory.
   */
  def createSnapshotFilter(path: Path, conf: Configuration): PathFilter = {
    val fs = path.getFileSystem(conf)
    val currentFiles = fs.listStatus(path).map(_.getPath).toSet
    val fileFilter = new PathFilter() {
      override def accept(path: Path) = {
        (!path.getName().startsWith(".") && !currentFiles.contains(path))
      }
    }
    fileFilter
  }

  def setSuperField(fieldName: String, fieldValue: Object, clazz : Object) {
    val field = clazz.getClass.getSuperclass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.set(clazz, fieldValue)
  }

}
