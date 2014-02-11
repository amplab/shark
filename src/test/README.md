###Hive Compatibility Test Warnings

#### Test results that rely on tables with `timestamp` fields may differ across JVM versions.
For example, these tests:
* udf5
* timestamp.1, timestamp_2, timestamp_udf

Pass when running with this JVM:
(Mac 10.9, AMPLab Jenkins)
java version "1.7.0_25"
Java(TM) SE Runtime Environment (build 1.7.0_25-b15)
Java HotSpot(TM) 64-Bit Server VM (build 23.25-b01, mixed mode)

But fail on EC2 when run with this JVM:
(EC2 c2.2xlarge)
java version "1.7.0_45"
OpenJDK Runtime Environment (amzn-2.4.3.2.32.amzn1-x86_64 u45-b15)
OpenJDK 64-Bit Server VM (build 24.45-b08, mixed mode)


A few more tests from test_pass.txt that fall into this category:
TestCliDriver_input_part8
TestSharkCliDriver: testCliDriver_timestamp_1
TestSharkCliDriver: testCliDriver_timestamp_2
TestSharkCliDriver: testCliDriver_timestamp_3
TestSharkCliDriver: testCliDriver_timestamp_udf
TestSharkCliDriver: testCliDriver_udf_to_unix_timestamp
TestSharkCliDriver: testCliDriver_udf5
