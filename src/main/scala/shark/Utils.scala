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

import java.util.Locale
import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration


object Utils {

  /**
   * Convert a memory quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def memoryBytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
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

}
