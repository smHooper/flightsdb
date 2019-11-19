
var editLine = function(newLineID) {
    // Remove current points
    lineSegmentLayers[currentLineID].markers.forEach(marker => map.removeLayer(marker))

    // Add the new points
    lineSegmentLayers[newLineID].markers.forEach(marker => marker.addTo(map)
}