import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class RichRow(row: Row) {
	def getAs[T](field): Option[T] =
		if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
# end def

class Trip(
	license,
	pickupTime: Long,
	dropoffTime: Long,
	pickupX,
	pickupY,
	dropoffX,
	dropoffY)

class RunGeoTime:

	def main(self):
		spark = SparkSession.builder().getOrCreate()

		taxiRaw = spark.read.option("header", "true").csv("taxidata")
		taxiParsed = taxiRaw.rdd.map(safe(parse))
		taxiGood = taxiParsed.map(_.left.get).toDS
		taxiGood.cache()

		hours = (pickup: Long, dropoff: Long) : {
			TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
		# end def
		hoursUDF = udf(hours)

		taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

		# register the UDF, use it in a where clause
		spark.udf.register("hours", hours)
		taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

		geojson = scala.io.Source.fromURL(this.getClass.getResource("/nyc-boroughs.geojson")).mkString

		features = geojson.parseJson.convertTo[FeatureCollection]
		areaSortedFeatures = features.sortBy { f : 
			borough = f("boroughCode").convertTo[Int]
			(borough, -f.geometry.area2D())
		# end def

		bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

		bLookup = (x, y) : {
			feature: Option[Feature] = bFeatures.value.find(f : {
				f.geometry.contains(Point(x, y))
			# end def)
			feature.map(f : {
				f("borough").convertTo[String]
			# end def).getOrElse("NA")
		# end def
		boroughUDF = udf(bLookup)

		taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
		taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")
		taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

		taxiGood.unpersist()

		sessions = taxiDone.
				repartition($"license").
				sortWithinPartitions($"license", $"pickupTime").
				cache()
		def boroughDuration(t1: Trip, t2: Trip): (String, Long):
			b = bLookup(t1.dropoffX, t1.dropoffY)
			d = (t2.pickupTime - t1.dropoffTime) / 1000
			(b, d)
		# end def

		boroughDurations =
			sessions.mapPartitions(trips : {
				iter: Iterator[Seq[Trip]] = trips.sliding(2)
				viter = iter.filter(_.size == 2).filter(p : p(0).license == p(1).license)
				viter.map(p : boroughDuration(p(0), p(1)))
			# end def).toDF("borough", "seconds")
		boroughDurations.
			where("seconds > 0").
			groupBy("borough").
			agg(avg("seconds"), stddev("seconds")).
			show()

		boroughDurations.unpersist()
	# end def

	def safe[S, T](f: S : T): S : Either[T, (S, Exception)]:
		Function[S, Either[T, (S, Exception)]] with Serializable {
			def apply(s: S): Either[T, (S, Exception)]:
				try {
					Left(f(s))
				# end def catch {
					case e: Exception : Right((s, e))
				# end def
			# end def
		# end def
	# end def

	def parseTaxiTime(rr: RichRow, timeField): Long:
		formatter = SimpleDateFormat(
			 "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
		optDt = rr.getAs[String](timeField)
		optDt.map(dt : formatter.parse(dt).getTime).getOrElse(0L)
	# end def

	def parseTaxiLoc(rr: RichRow, locField):
		rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
	# end def

	def parse(line: Row): Trip:
		rr = RichRow(line)
		Trip(
			license = rr.getAs[String]("hack_license").orNull,
			pickupTime = parseTaxiTime(rr, "pickup_datetime"),
			dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
			pickupX = parseTaxiLoc(rr, "pickup_longitude"),
			pickupY = parseTaxiLoc(rr, "pickup_latitude"),
			dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
			dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
		)
	# end def
# end def


if __name__ == '__main__':
	app = RunGeoTime()
	app.main()
# end if
