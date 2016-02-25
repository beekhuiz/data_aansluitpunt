__author__ = 'beekhuiz'

import ogr, osr


def transformPoly(inputPoly, inputEPSG, outputEPSG):
    """
    :param the coordinates of an input polygon in the inputEPSG (EPSG code) coordinate system
    the coordinates are transformed to the output EPSG code
    :return: a polygon with coordinates in the outputEPSG system
    """

    # create coordinate transformation
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(inputEPSG)

    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(outputEPSG)

    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    outputPoly = inputPoly
    outputPoly.Transform(coordTransform)
    return outputPoly


def createBBoxPoly(inputBBox):
    """
    Create a polygon from an input dictionary containing the bbox coordinates
    :param inputBBox:
    :return: a ogr.geometry polygon
    """
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(inputBBox['left'], inputBBox['bottom'])
    ring.AddPoint(inputBBox['left'], inputBBox['top'])
    ring.AddPoint(inputBBox['right'], inputBBox['top'])
    ring.AddPoint(inputBBox['right'], inputBBox['bottom'])
    ring.AddPoint(inputBBox['left'], inputBBox['bottom'])
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def transformPoint(x, y, inputEPSG, outputEPSG):
    """
    :param the x and y coordinates of a certain point in the inputEPSG (EPSG code) coordinate system
    the coordinates are transformed to the output EPSG code
    :return: a tuple with the x and y coordinates
    """

    # create a geometry from coordinates
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(x, y)

    # create coordinate transformation
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(inputEPSG)

    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(outputEPSG)

    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    point.Transform(coordTransform)    # transform point

    pointOutputEPSG = (point.GetX(), point.GetY())

    return pointOutputEPSG