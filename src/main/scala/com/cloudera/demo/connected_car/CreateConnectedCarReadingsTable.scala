package com.cloudera.demo.connected_car

import java.util
import java.util.ArrayList

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{PartialRow, CreateTableOptions, KuduClient}

/**
 * This will create a hash partitioned table in kudu
 * To store car reading information
 */
object CreateConnectedCarReadingsTable {

	def main(args:Array[String]):Unit = {
		if (args.length == 0) {
      println("Usage: <kudu master> <numberOfPartitions>")
      return
    }

		val kuduMaster = args(0)
    val numberOfPartitions = if (args.length > 1) args(1).toInt else 3
		val tableName = "connected_car_readings"

		val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()

		val columnList = new util.ArrayList[ColumnSchema]()

		columnList.add(new ColumnSchemaBuilder("time", Type.INT64).key(true).build())
		columnList.add(new ColumnSchemaBuilder("vin", Type.STRING).key(true).build())
		columnList.add(new ColumnSchemaBuilder("x_accel", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("y_accel", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("z_accel", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("speed", Type.INT8).key(false).build())
		columnList.add(new ColumnSchemaBuilder("brakes_on", Type.BOOL).key(false).build())
		columnList.add(new ColumnSchemaBuilder("signal_on", Type.BOOL).key(false).build())
		columnList.add(new ColumnSchemaBuilder("lane_departed", Type.BOOL).key(false).build())
		columnList.add(new ColumnSchemaBuilder("collision_detected", Type.BOOL).key(false).build())
		columnList.add(new ColumnSchemaBuilder("hazard_detected", Type.BOOL).key(false).build())
		columnList.add(new ColumnSchemaBuilder("latitude", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("longitude", Type.DOUBLE).key(false).build())

		val schema = new Schema(columnList)

    //See if table already exist
		if (kuduClient.tableExists(tableName)) {
			println("Table " + tableName + " already exists. Deleting it...")
			kuduClient.deleteTable(tableName)
			println("Table " + tableName + " deleted!")
		}

    //create a builder so we can set vin as a hash partition field
    val builder = new CreateTableOptions()
    val hashColumnList = new ArrayList[String]
    hashColumnList.add("vin")
    builder.addHashPartitions(hashColumnList, numberOfPartitions)

		println("Creating table " + tableName + "...")

		kuduClient.createTable(tableName, schema, null)
		println("Table " + tableName + " created!")

		kuduClient.shutdown()
	}
}
