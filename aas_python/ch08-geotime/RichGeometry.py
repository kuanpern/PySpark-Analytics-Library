
'''
 * A wrapper that provides convenience methods for using the spatial relations in the ESRI
 * GeometryEngine with a particular instance of the Geometry interface and an associated
 * SpatialReference.
 *
 * @param geometry the geometry object
 * @param spatialReference optional spatial reference; if not specified, uses WKID 4326 a.k.a.
 *												 WGS84, the standard coordinate frame for Earth.
'''
class RichGeometry(geometry: Geometry,
		spatialReference: SpatialReference = SpatialReference.create(4326)):

	def area2D() = geometry.calculateArea2D()

	def distance(other: Geometry):
		GeometryEngine.distance(geometry, other, spatialReference)
	# end def

	def contains(other: Geometry): Boolean:
		GeometryEngine.contains(geometry, other, spatialReference)
	# end def

	def within(other: Geometry): Boolean:
		GeometryEngine.within(geometry, other, spatialReference)
	# end def

	def overlaps(other: Geometry): Boolean:
		GeometryEngine.overlaps(geometry, other, spatialReference)
	# end def

	def touches(other: Geometry): Boolean:
		GeometryEngine.touches(geometry, other, spatialReference)
	# end def

	def crosses(other: Geometry): Boolean:
		GeometryEngine.crosses(geometry, other, spatialReference)
	# end def

	def disjoint(other: Geometry): Boolean:
		GeometryEngine.disjoint(geometry, other, spatialReference)
	# end def
# end def

'''
 * Helper class for implicitly creating RichGeometry wrappers
 * for a given Geometry instance.
'''
class RichGeometry:
	def createRichGeometry(g: Geometry): RichGeometry = RichGeometry(g)
# end def
