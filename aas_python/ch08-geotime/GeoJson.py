
class Feature(id: Option[JsValue],
									 properties: Map[String, JsValue],
									 geometry: RichGeometry) {
	def apply(): JsValue = properties(property)
	def get(): Option[JsValue] = properties.get(property)
# end class

class FeatureCollection(features: Array[Feature])
		extends IndexedSeq[Feature] {
	def apply(index): Feature = features(index)
	def length = features.length
# end class

class GeometryCollection(geometries: Array[RichGeometry])
		extends IndexedSeq[RichGeometry] {
	def apply(index): RichGeometry = geometries(index)
	def length = geometries.length
# end class

class GeoJsonProtocol extends DefaultJsonProtocol {
	class RichGeometryJsonFormat extends RootJsonFormat[RichGeometry] {
		def write(g: RichGeometry): JsValue:
			GeometryEngine.geometryToGeoJson(g.spatialReference, g.geometry).parseJson
		# end def
		def read(value: JsValue): RichGeometry:
			mg = GeometryEngine.geometryFromGeoJson(value.compactPrint, 0, Geometry.Type.Unknown)
			RichGeometry(mg.getGeometry, mg.getSpatialReference)
		# end def
	# end def

	class FeatureJsonFormat extends RootJsonFormat[Feature] {
		def write(f: Feature): Jsclass:
			buf = scala.collection.mutable.ArrayBuffer(
				"type" -> JsString("Feature"),
				"properties" -> JsObject(f.properties),
				"geometry" -> f.geometry.toJson)
			f.id.foreach(v : { buf += "id" -> v# end def)
			JsObject(buf.toMap)
		# end def

		def read(value: JsValue): Feature:
			jso = value.asJsObject
			id = jso.fields.get("id")
			properties = jso.fields("properties").asJsObject.fields
			geometry = jso.fields("geometry").convertTo[RichGeometry]
			Feature(id, properties, geometry)
		# end def
	# end def

	class FeatureCollectionJsonFormat extends RootJsonFormat[FeatureCollection] {
		def write(fc: FeatureCollection): Jsclass:
			JsObject(
				"type" -> JsString("FeatureCollection"),
				"features" -> JsArray(fc.features.map(_.toJson): _*)
			)
		# end def

		def read(value: JsValue): FeatureCollection:
			FeatureCollection(value.asJsObject.fields("features").convertTo[Array[Feature]])
		# end def
	# end def

	class GeometryCollectionJsonFormat extends RootJsonFormat[GeometryCollection] {
		def write(gc: GeometryCollection): Jsclass:
			JsObject(
				"type" -> JsString("GeometryCollection"),
				"geometries" -> JsArray(gc.geometries.map(_.toJson): _*))
		# end def

		def read(value: JsValue): GeometryCollection:
			GeometryCollection(value.asJsObject.fields("geometries").convertTo[Array[RichGeometry]])
		# end def
	# end def
# end def
