import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast

case class codesStats(DISTRICT: String, crime_type: String, count: Long)

object boston_guide {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", 0)
      .getOrCreate()

    import spark.implicits._

    val crimeFacts = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    val crimeCodes = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

    crimeCodes.dropDuplicates()

    val filteredCachedCrimeFacts = crimeFacts
      .select($"DISTRICT", $"YEAR", $"MONTH", $"OFFENSE_CODE", $"Lat", $"Long")
      .filter($"DISTRICT".isNotNull).cache()

//                    crimes_total - общее количество преступлений в этом районе

    val crimesTotal = filteredCachedCrimeFacts
      .select($"DISTRICT")
      .groupBy($"DISTRICT".as("ct_join"))
      .count()
      .withColumnRenamed("count", "crimes_total")

//                     crimes_monthly - медиана числа преступлений в месяц в этом районе

    val crimesMonthly = filteredCachedCrimeFacts
      .select($"DISTRICT", $"YEAR", $"MONTH")
      .groupBy($"DISTRICT", $"YEAR", $"MONTH").count()
      .selectExpr("DISTRICT", "percentile_approx(count, 0.5) over (partition by DISTRICT) as crimes_monthly").distinct()

//         frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую с одним пробелом “, ” , расположенных в порядке убывания частоты

    val offenseCodesBroadcast = broadcast(crimeCodes)

    val frequentCrimes = filteredCachedCrimeFacts
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
      .withColumn("crime_type", trim((split($"NAME", "-")).getItem(0)))
      .groupBy($"DISTRICT", $"crime_type")
      .count()
      .as[codesStats]
      .groupByKey(x => x.DISTRICT)
      .flatMapGroups {
        case (district, elements) =>
          elements.toList.sortBy(x => -x.count).take(3)
      }
      .withColumn(
        "frequent_crime_types", collect_list($"crime_type") over (Window.partitionBy($"DISTRICT"))
      )
      .select($"DISTRICT".as("fct_join"), $"frequent_crime_types")
      .distinct()

//                lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
    val latDistrict = filteredCachedCrimeFacts
      .filter($"Lat".isNotNull)
      .groupBy($"DISTRICT".as("avg_lat_join"))
      .agg(avg($"Lat"))
      .withColumnRenamed("avg(Lat)", "lat")

//                     lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
    val longDistrict = filteredCachedCrimeFacts
      .filter($"Long".isNotNull)
      .groupBy($"DISTRICT".as("avg_long_join"))
      .agg(avg($"Long"))
      .withColumnRenamed("avg(Long)", "lng")

    filteredCachedCrimeFacts.unpersist()

    crimesMonthly
      .join(crimesTotal, $"DISTRICT" === $"ct_join")
      .join(frequentCrimes, $"DISTRICT" === $"fct_join")
      .join(latDistrict, $"DISTRICT" === $"avg_lat_join")
      .join(longDistrict, $"DISTRICT" === $"avg_long_join")
      .drop("ct_join", "fct_join", "avg_lat_join", "avg_long_join")
      .write.parquet(args(2))

  }
}
