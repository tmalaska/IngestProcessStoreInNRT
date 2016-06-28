package com.cloudera.demo.connected_car

import java.util

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{CreateTableOptions, KuduClient}

import java.util.ArrayList

/**
 * This will create a hash partitioned table in kudu
 * To store car profile information
 */
object CreateConnectedCarProfileTable {

	def main(args:Array[String]):Unit = {
		if (args.length == 0) {
			println("Usage: <kudu master> <numberOfPartitions>")
			return
		}

		val kuduMaster = args(0)
    val numberOfPartitions = if (args.length > 1) args(1).toInt else 3

		val tableName = "connected_car_profiles"

		val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()

		val columnList = new util.ArrayList[ColumnSchema]()

		// TODO: join against another table with vin,year,make,model,owner

		columnList.add(new ColumnSchemaBuilder("vin", Type.STRING).key(true).build())
		columnList.add(new ColumnSchemaBuilder("acceleration_aggression_score", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("braking_aggression_score", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("handling_aggression_score", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("overall_aggression_score", Type.DOUBLE).key(false).build())
		columnList.add(new ColumnSchemaBuilder("oil_replacement_period", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("brake_replacement_period", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("tire_replacement_period", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("miles_count", Type.INT64).key(false).build())
		columnList.add(new ColumnSchemaBuilder("brakes_applied_count", Type.INT64).key(false).build())
		columnList.add(new ColumnSchemaBuilder("average_speed", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("illegal_lane_departure_plus_minus", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("collisions_count", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("hazards_detected_count", Type.INT32).key(false).build())
		columnList.add(new ColumnSchemaBuilder("readings_count", Type.INT64).key(false).build())
		columnList.add(new ColumnSchemaBuilder("accelerating_readings_count", Type.INT64).key(false).build())
		columnList.add(new ColumnSchemaBuilder("decelerating_readings_count", Type.INT64).key(false).build())
		columnList.add(new ColumnSchemaBuilder("last_updated", Type.INT64).key(false).build())

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
		kuduClient.createTable(tableName, schema, builder)
		println("Table " + tableName + " created!")

		kuduClient.shutdown()
	}
}
