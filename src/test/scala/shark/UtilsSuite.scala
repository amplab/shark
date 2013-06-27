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

import java.util.{HashMap => JHashMap}

import org.apache.hadoop.conf.Configuration

import org.scalatest.{BeforeAndAfter, FunSuite}


class UtilsSuite extends FunSuite {

  test("set aws credentials") {
    var conf = new Configuration
    var map = new JHashMap[String, String]()
    Utils.setAwsCredentials(conf, map)
    assert(conf.get("fs.s3n.awsAccessKeyId") === null)
    assert(conf.get("fs.s3n.awsSecretAccessKey") === null)
    assert(conf.get("fs.s3.awsAccessKeyId") === null)
    assert(conf.get("fs.s3.awsSecretAccessKey") === null)

    map.put("AWS_ACCESS_KEY_ID", "id")
    conf = new Configuration
    Utils.setAwsCredentials(conf, map)
    assert(conf.get("fs.s3n.awsAccessKeyId") === null)
    assert(conf.get("fs.s3n.awsSecretAccessKey") === null)
    assert(conf.get("fs.s3.awsAccessKeyId") === null)
    assert(conf.get("fs.s3.awsSecretAccessKey") === null)

    map.put("AWS_SECRET_ACCESS_KEY", "key")
    conf = new Configuration
    Utils.setAwsCredentials(conf, map)
    assert(conf.get("fs.s3n.awsAccessKeyId") === "id")
    assert(conf.get("fs.s3n.awsSecretAccessKey") === "key")
    assert(conf.get("fs.s3.awsAccessKeyId") === "id")
    assert(conf.get("fs.s3.awsSecretAccessKey") === "key")
  }

}
