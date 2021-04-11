import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import com.typesafe.config._

object CaseStudyAnalysis  extends App {
  val props=ConfigFactory.load()
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName(props.getString("applicationName"))
    .config("spark.master",props.getString("executionMode"))
    .getOrCreate()

  val path = args(0)

  val unitsSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("UNIT_DESC_ID",StringType),
    StructField("VEH_PARKED_FL",StringType),
    StructField("VEH_HNR_FL",StringType),
    StructField("VEH_LIC_STATE_ID",StringType),
    StructField("VIN",StringType),
    StructField("VEH_MOD_YEAR",StringType),
    StructField("VEH_COLOR_ID",StringType),
    StructField("VEH_MAKE_ID",StringType),
    StructField("VEH_MOD_ID",StringType),
    StructField("VEH_BODY_STYL_ID",StringType),
    StructField("EMER_RESPNDR_FL",StringType),
    StructField("OWNR_ZIP",StringType),
    StructField("FIN_RESP_PROOF_ID",StringType),
    StructField("FIN_RESP_TYPE_ID",StringType),
    StructField("VEH_DMAG_AREA_1_ID",StringType),
    StructField("VEH_DMAG_SCL_1_ID",StringType),
    StructField("FORCE_DIR_1_ID",StringType),
    StructField("VEH_DMAG_AREA_2_ID",StringType),
    StructField("VEH_DMAG_SCL_2_ID",StringType),
    StructField("FORCE_DIR_2_ID",StringType),
    StructField("VEH_INVENTORIED_FL",StringType),
    StructField("VEH_TRANSP_NAME",StringType),
    StructField("VEH_TRANSP_DEST",StringType),
    StructField("CONTRIB_FACTR_1_ID",StringType),
    StructField("CONTRIB_FACTR_2_ID",StringType),
    StructField("CONTRIB_FACTR_P1_ID",StringType),
    StructField("VEH_TRVL_DIR_ID",StringType),
    StructField("FIRST_HARM_EVT_INV_ID",StringType),
    StructField("INCAP_INJRY_CNT",IntegerType),
    StructField("NONINCAP_INJRY_CNT",IntegerType),
    StructField("POSS_INJRY_CNT",IntegerType),
    StructField("NON_INJRY_CNT",IntegerType),
    StructField("UNKN_INJRY_CNT",IntegerType),
    StructField("TOT_INJRY_CNT",IntegerType),
    StructField("DEATH_CNT",IntegerType)
  ))

  val primaryPersonSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("PRSN_NBR",IntegerType),
    StructField("PRSN_TYPE_ID",StringType),
    StructField("PRSN_OCCPNT_POS_ID",StringType),
    StructField("PRSN_INJRY_SEV_ID",StringType),
    StructField("PRSN_AGE",StringType),
    StructField("PRSN_ETHNICITY_ID",StringType),
    StructField("PRSN_GNDR_ID",StringType),
    StructField("PRSN_EJCT_ID",StringType),
    StructField("PRSN_REST_ID",StringType),
    StructField("PRSN_AIRBAG_ID",StringType),
    StructField("PRSN_HELMET_ID",StringType),
    StructField("PRSN_SOL_FL",StringType),
    StructField("PRSN_ALC_SPEC_TYPE_ID",StringType),
    StructField("PRSN_ALC_RSLT_ID",StringType),
    StructField("PRSN_BAC_TEST_RSLT",StringType),
    StructField("PRSN_DRG_SPEC_TYPE_ID",StringType),
    StructField("PRSN_DRG_RSLT_ID",StringType),
    StructField("DRVR_DRG_CAT_1_ID",StringType),
    StructField("PRSN_DEATH_TIME",StringType),
    StructField("INCAP_INJRY_CNT",IntegerType),
    StructField("NONINCAP_INJRY_CNT",IntegerType),
    StructField("POSS_INJRY_CNT",IntegerType),
    StructField("NON_INJRY_CNT",IntegerType),
    StructField("UNKN_INJRY_CNT",IntegerType),
    StructField("TOT_INJRY_CNT",IntegerType),
    StructField("DEATH_CNT",IntegerType),
    StructField("DRVR_LIC_TYPE_ID",StringType),
    StructField("DRVR_LIC_STATE_ID",StringType),
    StructField("DRVR_LIC_CLS_ID",StringType),
    StructField("DRVR_ZIP",StringType)
  ))

  val damagesSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("DAMAGED_PROPERTY",StringType)
  ))

  val chargesSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("PRSN_NBR",IntegerType),
    StructField("CHARGE",StringType),
    StructField("CITATION_NBR",StringType)
  ))

  val Primary_Person = spark.read
    .schema(primaryPersonSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Primary_Person_use.csv")

  val Units_use = spark.read
    .schema(unitsSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Units_use.csv")


  val Damages_use = spark.read
    .schema(damagesSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Damages_use.csv")

  val Charges_use = spark.read
    .schema(chargesSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Charges_use.csv")

  //1.	Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male
  val numCrashesWithKilledMales = Primary_Person.where(col("PRSN_INJRY_SEV_ID")==="KILLED" and col("PRSN_GNDR_ID")==="MALE")
                              .select("CRASH_ID").distinct.select(count("*").as("NumCrashesWithKilledMales"))
                              .write.mode(SaveMode.Overwrite).csv(s"$path/Number_Of_Crashes_With_Males_Killed.csv")

  //2.	Analysis 2: How many two wheelers are booked for crashes?
  val regexString = "MOTORCYCLE"

  //Crashes where two wheelers are involved
  val TwoWheelerCrashes = Units_use.select(
    col("*"),
    regexp_extract(col("VEH_BODY_STYL_ID"), regexString, 0).as("IsTwoWheeler")
  ).where(col("IsTwoWheeler") =!= "").drop("IsTwoWheeler")

  //Saving Two Wheeler Crash Count
  TwoWheelerCrashes.select(count(("*")) as "TwoWheelerCrashesCount")
    .write.mode(SaveMode.Overwrite).csv(s"$path/Number_Of_TwoWheeler_Crashes.csv")

  //3.	Analysis 3: Which state has highest number of accidents in which females are involved?
  val female_accidents = Primary_Person.where(col("PRSN_GNDR_ID")==="FEMALE").groupBy(col("DRVR_LIC_STATE_ID"))
    .agg(count("*") as "StateWiseFemaleAccidentCount").orderBy(desc("StateWiseFemaleAccidentCount"))

  female_accidents.drop("StateWiseFemaleAccidentCount").limit(1)
    .withColumnRenamed("DRVR_LIC_STATE_ID","State_With_Highest_Female_Accidents")
    .write.mode(SaveMode.Overwrite).csv(s"$path/State_With_Highest_Female_Accidents.csv")

  //4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
  Units_use.filter(col("VEH_MAKE_ID") =!= "NA").withColumn("NetInjuryCount",col("TOT_INJRY_CNT") + col("DEATH_CNT"))
    .groupBy(col("VEH_MAKE_ID")).agg(sum("NetInjuryCount").as("NetInjuriesPerMakeID"))
    .withColumn("rank",row_number().over(Window.orderBy(col("NetInjuriesPerMakeID").desc)))
    .filter(col("rank") > 4 and col("rank") < 16)

  // Calculating sum of total injury and total death count per VEH_MAKE_ID
  val NetInjuriesPerMakeID =  Units_use.filter(col("VEH_MAKE_ID") =!= "NA").withColumn("NetInjuryCount",col("TOT_INJRY_CNT") + col("DEATH_CNT"))
    .groupBy(col("VEH_MAKE_ID")).agg(sum("NetInjuryCount").as("NetInjuriesPerMakeID"))

  // ranking data as per totalcount of death and injury in descending order
  val myWindow = Window.orderBy(col("NetInjuriesPerMakeID").desc)
  val ranked_data = NetInjuriesPerMakeID.withColumn("rank",dense_rank().over(myWindow))

  // filtering rank between 5 to 15
  ranked_data.select("VEH_MAKE_ID").where(col("rank") >=5 and  col("rank") <=15)
  .write.mode(SaveMode.Overwrite).csv(s"$path/Vehicle_Make_ID.csv")

  //5. Analysis:5 For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
  Units_use.select("CRASH_ID","VEH_BODY_STYL_ID")
    .filter(col("VEH_BODY_STYL_ID") =!= "Unknown" and col("VEH_BODY_STYL_ID") =!= "NA" and col("VEH_BODY_STYL_ID") =!= "UNKNOWN" and col("VEH_BODY_STYL_ID") =!= "OTHER  (EXPLAIN IN NARRATIVE)" and col("VEH_BODY_STYL_ID") =!= "NOT REPORTED")
    .join(Primary_Person.select("CRASH_ID","PRSN_ETHNICITY_ID"),"CRASH_ID")
    .groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").agg(count("*").as("EthnicCount"))
    .withColumn("rank",row_number().over(Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("EthnicCount").desc)))
    .filter(col("rank") === 1).select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")
    .write.mode(SaveMode.Overwrite).csv(s"$path/Top_EthnicGroup_Per_BodyStyle.csv")

  //6. Analysis:6: Among the crashed cars, what are the Top 5 Zip Codes
  //   with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

  //Crashes where alcohol was the contributing Factor
  val regexForAlcoholCheck = "ALCOHOL"
  val FilteredAlcoholCrashes = Units_use.select(col("*"),
    regexp_extract(col("CONTRIB_FACTR_1_ID"), regexForAlcoholCheck, 0).as("ContribFactor1"),
    regexp_extract(col("CONTRIB_FACTR_2_ID"), regexForAlcoholCheck, 0).as("ContribFactor2"),
    regexp_extract(col("CONTRIB_FACTR_P1_ID"), regexForAlcoholCheck, 0).as("ContribFactorP1"))
    .where(col("ContribFactor1") =!= "" or col("ContribFactor2") =!= "" or col("ContribFactorP1") =!= "" )
    .drop("ContribFactor1","ContribFactor2","ContribFactorP1")

  // Joining above Filtered Crashes with Primary_Person to obtain DRIVER_ZIP
  val FilteredAlcoholCrashesWithDriverZip = Primary_Person.join(FilteredAlcoholCrashes,Primary_Person.col("CRASH_ID") === FilteredAlcoholCrashes.col("CRASH_ID"))
    .drop(Primary_Person.col("CRASH_ID"))

  // counting no of crashes per Driver ZipCode
  val countPerDriverZip = FilteredAlcoholCrashesWithDriverZip
    .groupBy((col("DRVR_ZIP")))
    .agg(count("*").as("CountOfCrashes"))

  val windowCountRank = Window.orderBy(desc("CountOfCrashes"))
  //Removing null Driver Zip values and selecting top 5 Zip Codes with highest number of crashes
  countPerDriverZip.na.drop.withColumn("rank_highest_count_first", dense_rank() over (windowCountRank))
    .where(col("rank_highest_count_first") <=5 ).select("DRVR_ZIP","CountOfCrashes")
    .write.mode(SaveMode.Overwrite).csv(s"$path/Top5ZipCodes_With_Alcohol_Based_Crashes.csv")

  //7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
  // and car avails Insurance
  val CrashesWithNoDamage = Units_use.join(Damages_use,Damages_use.col("CRASH_ID") === Units_use.col("CRASH_ID"),"left_anti")

  // Crashes where car avails Insurance and VEH_DMAG_SCL~ > 4
  val regexForInsuranceCheck = "INSURANCE"
  val FilteredDf = CrashesWithNoDamage.select(
    col("*"),
    regexp_extract(col("FIN_RESP_TYPE_ID"), regexForInsuranceCheck, 0).as("AvailsInsurance"),
  ).where(col("AvailsInsurance") =!= "" and substring(col("VEH_DMAG_SCL_1_ID"),9,1) > 4)

  // Count of distinct Crash IDs
  FilteredDf.dropDuplicates("CRASH_ID").select(count(col("CRASH_ID")).as("DistinctCrashesCount"))
    .write.mode(SaveMode.Overwrite).csv(s"$path/Count_Of_Distinct_CrashID.csv")

  //8 : Analysis 8:Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
  // has licensed Drivers, uses top 10 used vehicle colours
  // and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

  // filtering speeding related charges
  val speedRelatedCharges = Charges_use.withColumn("OffenceType",regexp_extract(col("CHARGE"),"SPEED",0))
    .filter(col("OffenceType") =!= "")

  // filtering Drivers with license
  val licensedDrivers = Primary_Person.withColumn("IsLicensed",regexp_extract(col("DRVR_LIC_TYPE_ID"),"DRIVER LIC",0))
    .filter(col("IsLicensed") =!= "")

  // Top 10 Vehicle Colours
  val top10colors = Units_use.groupBy(col("VEH_COLOR_ID")).agg(count("*").as("ColorCount"))
    .orderBy(col("ColorCount").desc).limit(10)

  // Top 25 States
  val top25states = Charges_use.groupBy(col("CRASH_ID")).agg(count("*").as("OffenceCount"))
    .join(Units_use.withColumn("VehicleType",regexp_extract(col("VEH_BODY_STYL_ID"),"CAR",0))
      .filter(col("VehicleType") =!= "")
      .select("CRASH_ID","VEH_LIC_STATE_ID")
      .filter(col("VEH_LIC_STATE_ID") =!= "NA")
      ,"CRASH_ID")
    .groupBy("VEH_LIC_STATE_ID").agg(sum("OffenceCount").as("StateWiseOffenceCount"))
    .withColumn("rank",row_number().over(Window.orderBy(col("StateWiseOffenceCount").desc)))
    .filter(col("rank") < 26)

  // left semi joining with base dataset as Unit_use dataset to get the matching records from left dataset only i.e. units_use
  Units_use.select("CRASH_ID","VEH_MAKE_ID")
    .join(speedRelatedCharges, Units_use.col("CRASH_ID") === speedRelatedCharges.col("CRASH_ID"),"left_semi")
    .join(licensedDrivers, Units_use.col("CRASH_ID") === licensedDrivers.col("CRASH_ID"),"left_semi")
    .join(top10colors, Units_use.col("VEH_COLOR_ID") === top10colors.col("VEH_COLOR_ID"),"left_semi")
    .join(top25states, Units_use.col("VEH_LIC_STATE_ID") === top25states.col("VEH_LIC_STATE_ID"),"left_semi")
    .groupBy(col("VEH_MAKE_ID"))
    .agg(count("*").as("TotalCount"))
    .orderBy(col("TotalCount").desc).select("VEH_MAKE_ID").limit(5)
    .write.mode(SaveMode.Overwrite).csv(s"$path/TopFiveVehicleMakes.csv")
}

