import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class MatchData:
	id_1,
	id_2,
	cmp_fname_c1: Option[Double],
	cmp_fname_c2: Option[Double],
	cmp_lname_c1: Option[Double],
	cmp_lname_c2: Option[Double],
	cmp_sex: Option[Int],
	cmp_bd: Option[Int],
	cmp_bm: Option[Int],
	cmp_by: Option[Int],
	cmp_plz: Option[Int],
	is_match: Boolean
# end class

class RunIntro:
	def main(self):
		spark = SparkSession.builder
			.appName("Intro")
			.getOrCreate
 
		preview = spark.read.csv("hdfs://user/ds/linkage")
		preview.show()
		preview.printSchema()

		parsed = spark.read
			.option("header", "true")
			.option("nullValue", "?")
			.option("inferSchema", "true")
			.csv("hdfs://user/ds/linkage")
		parsed.show()
		parsed.printSchema()

		parsed.count()
		parsed.cache()
		parsed.groupBy("is_match").count().orderBy($"count".desc).show()

		parsed.createOrReplaceTempView("linkage")
		spark.sql("""
			SELECT is_match, COUNT(*) cnt
			FROM linkage
			GROUP BY is_match
			ORDER BY cnt DESC
		""").show()

		summary = parsed.describe()
		summary.show()
		summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

		matches = parsed.where("is_match = true")
		misses = parsed.filter($"is_match" === false)
		matchSummary = matches.describe()
		missSummary = misses.describe()

		matchSummaryT = pivotSummary(matchSummary)
		missSummaryT = pivotSummary(missSummary)
		matchSummaryT.createOrReplaceTempView("match_desc")
		missSummaryT.createOrReplaceTempView("miss_desc")
		spark.sql("""
			SELECT a.field, a.count + b.count total, a.mean - b.mean delta
			FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
			ORDER BY delta DESC, total DESC
		""").show()

		matchData = parsed.as[MatchData]
		scored = matchData.map { md :
			(scoreMatchData(md), md.is_match)
		# end def.toDF("score", "is_match")
		crossTabs(scored, 4.0).show()
	# end def

	def crossTabs(scored, t):
		scored.
			selectExpr(s"score >= $t as above", "is_match").
			groupBy("above").
			pivot("is_match", Seq("true", "false")).
			count()
	# end def

	class Score(value) {
		def +(oi: Option[Int]):
			Score(value + oi.getOrElse(0))
		# end def
	# end def

	def scoreMatchData(md: MatchData):
		(Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
				md.cmp_by + md.cmp_bd + md.cmp_bm).value
	# end def

	def pivotSummary(desc):
		lf = longForm(desc)
		lf.groupBy("field").
			pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
			agg(first("value"))
	# end def

	def longForm(desc):
		schema = desc.schema
		desc.flatMap(row : {
			metric = row.getString(0)
			(1 until row.size).map(i : (metric, schema(i).name, row.getString(i).toDouble))
		# end def)
		.toDF("metric", "field", "value")
	# end def
# end def

if __name__ == '__main__':
	app = RunIntro()
	app.main()
# end if
