package lk.uom.datasearch.bhagya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{rand, _}

object SampleCreationWithFilter {

  /*
   * Run on server:
   * spark-submit --class lk.uom.datasearch.bhagya.SampleCreation /home/buddhi13/TLM/Work/Bhagya/cdr/jobs/cdr-sample-generation_2.11-1.4.2.jar /SCDR
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampleCreation")
      .config("spark.sql.broadcastTimeout", "36000")
      .getOrCreate()

    var dataRoot = "/SCDR"

    if (args(0) != null) {
      dataRoot = args(0)
    }
    println("data root : ", dataRoot)

    val dataDirectory = dataRoot + "/generated_voice_records_20130601_20131201"
    val locationsCsv = dataRoot + "/1K_CELL_CENTERS_ALL.CSV"
    val outputLocation = "/MSC/buddhi13/bhagya/cdr/output/400K-SampleWithFilterForWesternProvince"
    val startDate = "2013-07-01"
    val endDate = "2013-07-31"
    val userCount = 400000

    // Reading parquet data
    var cdrDF = spark.read.parquet(dataDirectory)
      .select(
        col("SUBSCRIBER_ID"),
        to_date(col("CALL_TIME"), "yyyyMMddHHmmss").as("CALL_TIMESTAMP"),
        col("CALL_TIME"),
        col("INDEX_1KM")
      )

    val locationsCsvDF = spark.read.option("header", "true").csv(locationsCsv)
      .select(
        col("LOCATION_ID").as("LOCATION_ID_CSV"),
        col("LONGITUDE"),
        col("LATITUDE"),
        col("DSD_CODE4"),
        col("PROVINCE_NAME"),
        col("DISTRICT_NAME"),
        col("DS_NAME"),
        col("BTS_AREA")
      )

    val locationsDF = spark.read.option("header", "true").csv(locationsCsv)
      .select(
        col("LOCATION_ID"),
        col("PROVINCE_NAME")
      )

    val selectedAreaDF = locationsDF.filter(col("PROVINCE_NAME").equalTo("Western"))

    val pickedTimeIntervalDF = cdrDF.filter(col("CALL_TIMESTAMP").between(startDate, endDate))

    val restrictedToAreaDF = pickedTimeIntervalDF
      .join(selectedAreaDF, selectedAreaDF("LOCATION_ID") === pickedTimeIntervalDF("INDEX_1KM"))
      .select(
        col("SUBSCRIBER_ID"),
        col("CALL_TIMESTAMP"),
        col("CALL_TIME"),
        col("LOCATION_ID")
      )

    val countCallsPerDaysDF = restrictedToAreaDF
      .select(
        col("SUBSCRIBER_ID"),
        date_format(col("CALL_TIMESTAMP"), "D").cast("int")
          .as("DAY")
      )
      .groupBy(col("SUBSCRIBER_ID"), col("DAY")).count()

    val usersWithCDREveryDay = countCallsPerDaysDF.groupBy(col("SUBSCRIBER_ID")).sum("DAY")
      .filter(col("sum(DAY)").equalTo("6107"))

    val filteredAllUsers = usersWithCDREveryDay.select(col("SUBSCRIBER_ID") as "FILT_SUBSCRIBER_ID").distinct()

    // Restricting to random n, users
    // Renaming column to avoid duplication in next step
    val randomlyPickedUsers = filteredAllUsers.orderBy(rand()).limit(userCount)

    val cdrForRandomlyPickedUsers = restrictedToAreaDF.
      join(randomlyPickedUsers, randomlyPickedUsers("FILT_SUBSCRIBER_ID") === restrictedToAreaDF("SUBSCRIBER_ID"))
      .select(
        col("SUBSCRIBER_ID"),
        col("CALL_TIME"),
        col("LOCATION_ID")
      )

    val cdrForRandomlyPickedUsersWithLocationDetails = cdrForRandomlyPickedUsers.
      join(locationsCsvDF, locationsCsvDF("LOCATION_ID_CSV") === cdrForRandomlyPickedUsers("LOCATION_ID"))
      .select(
        col("SUBSCRIBER_ID"),
        col("CALL_TIME"),
        col("LOCATION_ID"),
        col("LONGITUDE"),
        col("LATITUDE"),
        col("DSD_CODE4"),
        col("PROVINCE_NAME"),
        col("DISTRICT_NAME"),
        col("DS_NAME"),
        col("BTS_AREA")
      )

    // Writing Output
    cdrForRandomlyPickedUsersWithLocationDetails.coalesce(1).write.option("header", "true").csv(outputLocation)
  }
}
