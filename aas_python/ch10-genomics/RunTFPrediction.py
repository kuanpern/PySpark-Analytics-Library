import pyspark.conf
import pyspark.sql
SparkConf = pyspark.conf.SparkConf
SparkSession = pyspark.sql.SparkSession

class RunTFPrediction {
	def main(self):
		sc = SparkContext(SparkConf().setAppName("TF Prediction"))

		# configuration
		hdfsPrefix = "/user/ds/genomics"
		localPrefix = "/user/ds/genomics"

		# Set up broadcast variables for computing features along with some
		# utility functions

		# Load the human genome reference sequence
		bHg19Data = sc.broadcast(
			TwoBitFile(
				LocalFileByteAccess(
					File(Paths.get(localPrefix, "hg19.2bit").toString))))

		# fn for finding closest transcription start site
		# naive; exercise for reader: make this faster
		def distanceToClosest(loci: Vector[Long], query: Long): Long:
			loci.map(x : math.abs(x - query)).min
		# end def

		# CTCF PWM from https:#dx.doi.org/10.1016/j.cell.2012.12.009
		# generated with genomics/src/main/python/pwm.py
		bPwmData = sc.broadcast(Vector(
			Map('A'->0.4553,'C'->0.0459,'G'->0.1455,'T'->0.3533),
			Map('A'->0.1737,'C'->0.0248,'G'->0.7592,'T'->0.0423),
			Map('A'->0.0001,'C'->0.9407,'G'->0.0001,'T'->0.0591),
			Map('A'->0.0051,'C'->0.0001,'G'->0.9879,'T'->0.0069),
			Map('A'->0.0624,'C'->0.9322,'G'->0.0009,'T'->0.0046),
			Map('A'->0.0046,'C'->0.9952,'G'->0.0001,'T'->0.0001),
			Map('A'->0.5075,'C'->0.4533,'G'->0.0181,'T'->0.0211),
			Map('A'->0.0079,'C'->0.6407,'G'->0.0001,'T'->0.3513),
			Map('A'->0.0001,'C'->0.9995,'G'->0.0002,'T'->0.0001),
			Map('A'->0.0027,'C'->0.0035,'G'->0.0017,'T'->0.9921),
			Map('A'->0.7635,'C'->0.0210,'G'->0.1175,'T'->0.0980),
			Map('A'->0.0074,'C'->0.1314,'G'->0.7990,'T'->0.0622),
			Map('A'->0.0138,'C'->0.3879,'G'->0.0001,'T'->0.5981),
			Map('A'->0.0003,'C'->0.0001,'G'->0.9853,'T'->0.0142),
			Map('A'->0.0399,'C'->0.0113,'G'->0.7312,'T'->0.2177),
			Map('A'->0.1520,'C'->0.2820,'G'->0.0082,'T'->0.5578),
			Map('A'->0.3644,'C'->0.3105,'G'->0.2125,'T'->0.1127)))

		# compute a motif score based on the TF PWM
		def scorePWM(ref):
			score1 = (ref.sliding(bPwmData.value.length)
				.map(s : {
					s.zipWithIndex.map(p : bPwmData.value(p._2)(p._1)).product# end def)
				.max)
			rc = Alphabet.dna.reverseComplementExact(ref)
			score2 = (rc.sliding(bPwmData.value.length)
				.map(s : {
					s.zipWithIndex.map(p : bPwmData.value(p._2)(p._1)).product# end def)
				.max)
			math.max(score1, score2)
		# end def

		# build in-memory structure for computing distance to TSS
		# we are essentially manually implementing a broadcast join here
		tssRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, "gencode.v18.annotation.gtf").toString).rdd
			.filter(_.getFeatureType == "transcript")
			.map(f : (f.getContigName, f.getStart)))
		# this broadcast variable will hold the broadcast side of the "join"
		bTssData = sc.broadcast(tssRDD
			# group by contig name
			.groupBy(_._1)
			# create Vector of TSS sites for each chromosome
			.map(p : (p._1, p._2.map(_._2.toLong).toVector))
			# collect into local in-memory structure for broadcasting
			.collect().toMap)

		# load conservation data; independent of cell line
		phylopRDD = (sc.loadParquetFeatures(Paths.get(hdfsPrefix, "phylop").toString).rdd
			# clean up a few irregularities in the phylop data
			.filter(f : f.getStart <= f.getEnd)
			.map(f : (ReferenceRegion.unstranded(f), f)))

		# MAIN LOOP

		cellLines = Vector("GM12878", "K562", "BJ", "HEK293", "H54", "HepG2")

		dataByCellLine = cellLines.map(cellLine : {
			dnaseRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, s"dnase/$cellLine.DNase.narrowPeak").toString).rdd
				.map(f : ReferenceRegion.unstranded(f)).map(r : (r, r)))

			# add label (TF bound is true, unbound is false)
			chipseqRDD = (sc.loadFeatures(Paths.get(hdfsPrefix, s"chip-seq/$cellLine.ChIP-seq.CTCF.narrowPeak").toString).rdd
				.map(f : ReferenceRegion.unstranded(f)).map(r : (r, r)))
			dnaseWithLabelRDD = (LeftOuterShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
				.partitionAndJoin(dnaseRDD, chipseqRDD)
				.map(p : (p._1, p._2.size))
				.reduceByKey(_ + _)
				.map(p : (p._1, p._2 > 0))
				.map(p : (p._1, p)))

			# add conservation data
			def aggPhylop(values: Vector[Double]):
				avg = values.sum / values.length
				m = values.min
				M = values.max
				(avg, m, M)
			# end def
			dnaseWithPhylopRDD = (LeftOuterShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
				.partitionAndJoin(dnaseRDD, phylopRDD)
				.filter(!_._2.isEmpty)
				.map(p : (p._1, p._2.get.getScore.doubleValue))
				.groupByKey()
				.map(p : (p._1, aggPhylop(p._2.toVector))))

			# build final training example RDD
			examplesRDD = (InnerShuffleRegionJoin(bHg19Data.value.sequences, 1000000, sc)
				.partitionAndJoin(dnaseWithLabelRDD, dnaseWithPhylopRDD)
				.map(tup : (tup._1, tup._2, bHg19Data.value.extract(tup._1._1)))
				.filter(!_._3.contains("N"))
				.map(tup : {
					region = tup._1._1
					label = tup._1._2
					contig = region.referenceName
					start = region.start
					end = region.end
					phylopAvg = tup._2._1
					phylopMin = tup._2._2
					phylopMax = tup._2._3
					seq = tup._3
					pwmScore = scorePWM(seq)
					closestTss = math.min(
						distanceToClosest(bTssData.value(contig), start),
						distanceToClosest(bTssData.value(contig), end))
					tf = "CTCF"
					(contig, start, end, pwmScore, phylopAvg, phylopMin, phylopMax, closestTss, tf, cellLine, label)# end def))
			examplesRDD
		# end def)

		# union the prepared data together
		preTrainingData = dataByCellLine.reduce(_ ++ _)
		preTrainingData.cache()
		preTrainingData.count() # 802059
		preTrainingData.filter(_._11 == true).count() # 220344

		# carry on into classification
	# end def
# end def


if __name__ == '__main__':
	app = RunTFPrediction()
	app.main()
# end if
