const dataSteward = 'dena_flight_data@nps.gov';
var zoomMapCalled = 0;
const noFileGIFS = [
	"https://media2.giphy.com/media/l2Je0ihsoThQy6D0Q/giphy.gif?cid=790b76112a7424c801a727f0e2d02ccc6731809cafdc8d60&rid=giphy.gif",
    "https://media.tenor.com/images/ea9f942522f48f3897999cda42778e6a/tenor.gif",
    "https://media.tenor.com/images/18bc79d054124aa3d8c594f6555383ed/tenor.gif" 
];

/*
Modify the default boxzoom Leaflet.Handler to use the same functionality for selection
*/
L.Map.Selector = L.Map.BoxZoom.extend({
	initialize: function (map) {
		this._map = map;
		this._container = map._container;
		this._pane = map._panes.overlayPane;
	},

	addHooks: function () {
		L.DomEvent.on(this._container, 'mousedown', this._onMouseDown, this);
	},

	removeHooks: function () {
		L.DomEvent.off(this._container, 'mousedown', this._onMouseDown);
	},

	_onMouseDown: function (e) {
		if (!e.shiftKey || ((e.which !== 1) && (e.button !== 1))) { return false; }

		L.DomUtil.disableTextSelection();

		this._startLayerPoint = this._map.mouseEventToLayerPoint(e);

		this._box = L.DomUtil.create('div', 'leaflet-zoom-box', this._pane);
		L.DomUtil.setPosition(this._box, this._startLayerPoint);

		//TODO refactor: move cursor to styles
		this._container.style.cursor = 'crosshair';

		L.DomEvent
		    .on(document, 'mousemove', this._onMouseMove, this)
		    .on(document, 'mouseup', this._onMouseUp, this)
		    .on(document, 'keydown', this._onKeyDown, this)
		    .preventDefault(e);

		this._map.fire('boxzoomstart');
	},

	_onMouseMove: function (e) {
		var startPoint = this._startLayerPoint,
		    box = this._box,

		    layerPoint = this._map.mouseEventToLayerPoint(e),
		    offset = layerPoint.subtract(startPoint),

		    newPos = new L.Point(
		        Math.min(layerPoint.x, startPoint.x),
		        Math.min(layerPoint.y, startPoint.y));

		L.DomUtil.setPosition(box, newPos);

		// TODO refactor: remove hardcoded 4 pixels
		box.style.width  = (Math.max(0, Math.abs(offset.x) - 4)) + 'px';
		box.style.height = (Math.max(0, Math.abs(offset.y) - 4)) + 'px';
	},

	_finish: function () {
		this._pane.removeChild(this._box);
		this._container.style.cursor = '';

		L.DomUtil.enableTextSelection();

		L.DomEvent
		    .off(document, 'mousemove', this._onMouseMove)
		    .off(document, 'mouseup', this._onMouseUp)
		    .off(document, 'keydown', this._onKeyDown);
	},

	_onMouseUp: function (e) {

		this._finish();

		var map = this._map;
		var layerPoint = map.mouseEventToLayerPoint(e);

		if (this._startLayerPoint.equals(layerPoint)) { return; }

		var bounds = new L.LatLngBounds(
		        map.layerPointToLatLng(this._startLayerPoint),
		        map.layerPointToLatLng(layerPoint));

		//map.fitBounds(bounds);
		console.log(bounds);

		map.fire('boxzoomend', {
			boxZoomBounds: bounds
		});
	},

	_onKeyDown: function (e) {
		if (e.keyCode === 27) {
			this._finish();
		}
	}

})

L.Map.mergeOptions({selector: true});
L.Map.addInitHook('addHandler', 'selector', L.Map.Selector);
L.Map.mergeOptions({boxZoom: false});

function deepCopy(inObject) {
  let outObject, value, key

  if (typeof inObject !== "object" || inObject === null) {
    return inObject // Return the value if inObject is not an object
  }

  // Create an array or object to hold the values
  outObject = Array.isArray(inObject) ? [] : {}

  for (key in inObject) {
    value = inObject[key]

    // Recursively (deep) copy for nested objects, including arrays
    outObject[key] = deepCopy(value)
  }

  return outObject
}


function getColor() {
	var color = Math.floor(Math.random() * 16777216).toString(16);
	var hexColor = '#000000'.slice(0, -color.length) + color;

	return hexColor;
}


function getSelectedFileName() {
	try {
		return $('.card.selected .card-header .card-link > .card-title').text();
	} catch {
		return '';
	}
}


function toggleZoomNextPreviousButtons() {

	if (mapExtentBuffer.length > 1 && currentMapExtentIndex !== 0) {
		$('#img-zoom_previous').parent().removeClass('leaflet-toolbar-icon-disabled');
	} else {
		$('#img-zoom_previous').parent().addClass('leaflet-toolbar-icon-disabled');
	}

	if (mapExtentBuffer.length > currentMapExtentIndex + 1) {
		$('#img-zoom_next').parent().removeClass('leaflet-toolbar-icon-disabled');
	} else {
		$('#img-zoom_next').parent().addClass('leaflet-toolbar-icon-disabled');
	}
	//$('#img-zoom_previous').css('opacity', mapExtentBuffer.length > 1 && currentMapExtentIndex !== 0 ? 1 : 0.35);
	//$('#img-zoom_next').css('opacity', mapExtentBuffer.length > currentMapExtentIndex + 1 ? 1 : 0.35);

}


function onMapZoom() {
	/* 
	when the map changes extent, add the extent to the zoom buffer
	so users can navigate to and from previous extents
	*/

	try {
		var currentBounds = map.getBounds();
	} catch {
		// bounds haven't been set yet
		return;
	}

	var previousMapIndex = currentMapExtentIndex;
	if (mapExtentBuffer[previousMapIndex] != currentBounds) {
		currentMapExtentIndex ++;
		mapExtentBuffer.push(currentBounds);
		mapExtentBuffer = mapExtentBuffer.slice(0, currentMapExtentIndex + 1);
	}

	toggleZoomNextPreviousButtons();

}


function zoomMap() {

	zoomMapCalled ++; // should never exceed 1

	var zoomTo = mapExtentBuffer[currentMapExtentIndex];
	var currentBuffer = [...mapExtentBuffer];// copy the buffer
	map.fitBounds(zoomTo);

	//fitBounds() will trigger onMapZoom, so reset what it did
	// 	delay this by 1 seconds so the move event has time to fire, 
	//	because it happens asynchronously
	setTimeout(
		function() {
			mapExtentBuffer = [...currentBuffer];
			currentMapExtentIndex = currentMapExtentIndex > 0 ? currentMapExtentIndex - 1 : 0;
			// Set style here too because buffer might not be accurate in onMapZoom()
			$('#img-zoom_previous').css('opacity', mapExtentBuffer.length > 1 && currentMapExtentIndex !== 0 ? 1 : 0.35);
			$('#img-zoom_next').css('opacity', mapExtentBuffer.length > currentMapExtentIndex + 1 ? 1 : 0.35);
			zoomMapCalled --; // reset value so callers no map finished moving
		},
		1000);

}


function onPreviousExtentClick() {
	
	// if the map is currently moving, do nothing
	if (zoomMapCalled) return; 

	if (currentMapExtentIndex >= 1) {
		
		currentMapExtentIndex --;
		zoomMap();
	}
}


function onNextExtentClick() {

	// if the map is currently moving, do nothing
	if (zoomMapCalled) return;

	var bufferLength = mapExtentBuffer.length;

	// Use separate conditional test to ensure that if current extent is the second to last 
	//	(and it will be the last) that the button looks disabled
	if (currentMapExtentIndex >= bufferLength - 1) {
		currentMapExtentIndex = bufferLength - 1;
	} else {
		currentMapExtentIndex ++;			
		zoomMap();
	}
}



function splitAtVertex(segmentID, vertexID, minVertexIndex, isRedo=false){
	/*
	Split a line at the associated vertex. vertexID is the ID of the 
	point from the geojson used to create it
	*/

	var fileName = getSelectedFileName();//$('.collapse.show').text();
	var originalLine = lineLayers[fileName][segmentID];
	var allLatlngs = originalLine.getLatLngs();

	// vertexID is a global ID so calculate the index within the latlngs array
	var vertexIndex = vertexID - minVertexIndex;

	if ((vertexIndex === allLatlngs.length - 1) || (vertexIndex === 0)) {
		alert('Invalid operation: You attempted to split this line at the end of this track segment');
		return;
	}

	var originalLatlngs = allLatlngs.slice(0, (vertexIndex + 1));
	var newLatLngs = allLatlngs.slice(vertexIndex);
	
	// Set coords for the old line (everything up to the vertex) and 
	//  create the new line (the new vertex and everything after)
	originalLine.setLatLngs(originalLatlngs);
	var newSegmentID = Math.max.apply(null, Object.keys(lineLayers[fileName])) + 1;
	var newColor = getColor();
	colors[fileName][newSegmentID] = newColor;
	var newLine = L.polyline(
		newLatLngs, 
		options={color: newColor})
	.addEventListener({click: onLineClick})
	.addTo(map);
	lineLayers[fileName][newSegmentID] = newLine;

	// Create the new point geojson by looping through all points 
	//  of the original and adding each .feature to the json.
	//	It's inefficient to recreate the original points too, but 
	//	if I don't there's some reference to the old points that 
	//	means the min_index changes in lines that were split before.
	var newDepartureTime = pointGeojsonLayers[fileName][segmentID].toGeoJSON().features[vertexIndex].properties.ak_datetime
	var newGeoJSON = {
		type: "FeatureCollection",
		features: []
	}
	var originalGeoJSON = {
		type: "FeatureCollection",
		features: []
	}
	pointGeojsonLayers[fileName][segmentID].eachLayer(
		function(layer) {
			var featureID = layer.feature.id;
			let newFeature = deepCopy(layer.feature);

			// all points up to and including the splitting vertex
			if (featureID <= vertexID) {
				layer.feature.properties.landing_datetime = newDepartureTime;
				originalGeoJSON.features.push(layer.feature);
			} 
			// the splitting vertex and all points after it
			if (featureID >= vertexID) {
				// set the mapID to the new segment ID so the points 
				//  are still related to the line
				newFeature.properties.mapID = newSegmentID;
				newFeature.properties.min_index = vertexID;
				newFeature.properties.departure_datetime = newDepartureTime;
				if (parseInt(featureID) === parseInt(vertexID)) newFeature.properties.is_new_segment = true;
				newGeoJSON.features.push(newFeature);
			}

			pointGeojsonLayers[fileName][segmentID].removeLayer(layer);
		}
	)

	// Add the newly create geojson layers
	pointGeojsonLayers[fileName][segmentID] = L.geoJSON(originalGeoJSON, { 
		onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
		pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, colors[fileName][segmentID])),
		style: {className: 'cut-cursor-eligible'}
	});
	pointGeojsonLayers[fileName][newSegmentID] = L.geoJSON(newGeoJSON, { 
		onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
		pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, newColor)),
		style: {className: 'cut-cursor-eligible'}
	});


	// Update the legend
	var thisInfo = JSON.parse(JSON.stringify(trackInfo[fileName][segmentID])); //only way to deep copy without maintaining reference to original
	thisInfo['visible'] = true;
	trackInfo[fileName][newSegmentID] = thisInfo;
	showVertices(newSegmentID);
	updateLegend(fileName);
	
	isEditing[fileName] = true;
	
	// If the user is not redoing the edit action (i.e., pressed ctr + shift + Z or the redo button), 
	//	reset the buffer because this is a new action
	if (!isRedo) {
		redoBuffer = [];
		editingBufferIndex ++;
		undoBuffer[editingBufferIndex] = {
			function: undoSplitAtVertex,
			args: {
				fileName: fileName,
				segmentID: segmentID,
				mergeSegID: newSegmentID
			}
		}
		toggleUndoButton();
	}
}


function undoSplitAtVertex({fileName, segmentID, mergeSegID}) {

	if (fileName !== getSelectedFileName()) {
		fileWasSelected(fileName);
		loadTracksFromMemory(fileName)
	    	.then(() => {hideLoadingIndicator('loadTracksFromMemory')});
	}

	if (selectedLines[fileName] == segmentID) {
		hideVertices(segmentID);
	}
	// Get the vertex ID where the split occurred so the split can be redone 
	//	if the user wants (i.e., presses ctr + shift + Z)
	let mergeGeojson = pointGeojsonLayers[fileName][mergeSegID].toGeoJSON();
	let splitVertexID = mergeGeojson.features[0].properties.min_index;

	// Add the lat/lngs of the newer line to the original line
	let allLatlngs = lineLayers[fileName][segmentID].getLatLngs();
	allLatlngs.push(...lineLayers[fileName][mergeSegID].getLatLngs().slice(1));//don't use 0th coord because it's duplicated
	lineLayers[fileName][segmentID].setLatLngs(allLatlngs);

	let originalGeojson = pointGeojsonLayers[fileName][segmentID].toGeoJSON();
	let properties = originalGeojson.features[0].properties
	const departureDatetime = properties.departure_datetime;
	const landingDatetime = mergeGeojson.features[0].properties.landing_datetime;
	const minIndex = properties.min_index;
	pointGeojsonLayers[fileName][mergeSegID].eachLayer(
		function(layer) {
			var featureID = layer.feature.id;
			if (parseInt(featureID) === parseInt(splitVertexID)) return;

			var theseProperties = layer.feature.properties //only creates reference, not copy
			// set the mapID to the old segment ID so the points 
			//  are still related to the line
			theseProperties.mapID = segmentID;
			theseProperties.min_index = minIndex;
			theseProperties.departure_datetime = departureDatetime;
			theseProperties.landing_datetime = landingDatetime;
			theseProperties.is_new_segment = false;
			// add this feature to the original
			originalGeojson.features.push(layer.feature);
		}
	)

	// Reset colors and event handler for when the points are clicked again
	pointGeojsonLayers[fileName][segmentID] = L.geoJSON(originalGeojson, { 
		onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
		pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, colors[fileName][segmentID])),
		style: {className: 'cut-cursor-eligible'}
	});

	// delete the old stuff
	deleteTrack(mergeSegID, showAlert=false, isRedo=true);

	// Show the newly merged line as selected
	showVertices(segmentID);
	selectLegendItem(segmentID);

	redoBuffer[editingBufferIndex - 1] = {
		function: redoSplitAtVertex,
		args: {
			fileName: fileName,
			segmentID: segmentID,
			vertexID: splitVertexID,
			minVertexIndex: minIndex
		}
	}

}


function redoSplitAtVertex({fileName, segmentID, vertexID, minVertexIndex}) {

	if (fileName !== getSelectedFileName()) {
		fileWasSelected(fileName);
		loadTracksFromMemory(fileName)
	    	.then(() => {hideLoadingIndicator('loadTracksFromMemory')});
	}

	splitAtVertex(segmentID, vertexID, minVertexIndex, isRedo=true);
}


function onVertexClick(event, segmentID, vertexID, minVertexIndex) {
	if (event.originalEvent.ctrlKey || $('#img-split_vertex').parent().hasClass('map-tool-selected')) {
		splitAtVertex(segmentID, vertexID, minVertexIndex)
	}
}


function onEachPoint(feature, layer, fileName) {
	/*
	For each point, add the coordinates to a dictionary of 
	segmentID: [coords] so lines can be created later  
	*/
	var properties = feature.properties
	var segmentID = properties.segment_id;
	
	// mapID is used to keep track of lines and points on the map. 
	//  They could be split, which would make the mapID different 
	//  from the segment_id. If the mapID is already set though, 
	//  that means that this feature was created by splitAtVertex(), 
	//  so don't reassign it
	if (feature.properties["mapID"] == undefined) {
		feature.properties["mapID"] = segmentID;
	}
	
	layer.addEventListener({
		click: (e => onVertexClick(e, properties.mapID, properties.point_index, properties.min_index))
	});

	// Add coordinates to dict of coordinate arrays for creating lines later
	var pointCoords = feature.geometry.coordinates;
	if (lineCoords[fileName][segmentID] === undefined) {
		// coords in geojson are [x, y] but polyline needs [lat, lon]
		lineCoords[fileName][segmentID] = [[pointCoords[1], pointCoords[0]]]
	} else {
		lineCoords[fileName][segmentID].push([pointCoords[1], pointCoords[0]])
	}

}


function geojsonPointAsCircle(feature, latlng, color) {
	var markerOptions = {
		radius: 8,
		weight: 1,
		opacity: 1,
		fillOpacity: 0.8,
		fillColor: color,
		color: color
	}

	var popup = L.popup({
		autoPan: false,

	})
		.setContent(`
			<div>
				<p><strong>Time:</strong> ${feature.properties.ak_datetime}</p>
				<p><strong>Altitude:</strong> ${feature.properties.altitude_ft} ft</p>
				<p><strong>Speed:</strong> ${feature.properties.knots} kn</p>
				<p><strong>Heading:</strong> ${feature.properties.heading}°</p>
			</div>
		`);

	return L.circleMarker(latlng, markerOptions)
		.bindPopup(popup);
}


function showVertices(id, hideCurrent=true) {

	var fileName = getSelectedFileName();

	var currentLineID = selectedLines[fileName];
	if (currentLineID >= 0 && hideCurrent) {
		hideVertices(currentLineID);
	}
	
	var geojsonPoints = pointGeojsonLayers[fileName][id];
	map.addLayer(geojsonPoints);

	// layer.bringToFront() requires that the layers are added to the 
	//  map div first, which doesn't happen until the map view is set. 
	//  This can either happen with map.setView() or map.fitBounds().
	var layerBounds = geojsonPoints.getBounds();
	try {
		// First try to set the view as the current view (i.e., don't move the map).
		//  This fails if the map view hasn't been set yet and therefore doesn't 
		//  have a "center"
		map.setView(map.getCenter());
	} catch {
		// If that doesn't work, fit the map to the currently selected layer
		map.fitBounds(layerBounds);
	}
	
	// If the layer isn't visible within the view, set the view to 
	//  the entire file extent
	if (!map.getBounds().intersects(layerBounds)) {
		map.fitBounds(fileExtents[fileName]);
	}

	lineLayers[fileName][id].bringToFront();
	geojsonPoints.bringToFront();

	selectedLines[fileName] = id;
	$('#img-zoom_selected').parent().removeClass('leaflet-toolbar-icon-disabled');

	// Remove the cut cursor on hover for the previosu track and show the cut icon on hover for this
	//$('.cut-cursor-eligible').removeClass('cut-cursor-enabled')
	$('#img-split_vertex').parent().hasClass('map-tool-selected') ? 		
		$('.cut-cursor-eligible').addClass('cut-cursor-enabled'):
		$('.cut-cursor-enabled').removeClass('cut-cursor-enabled');
	
}


function showLoadingIndicator(timeout=15000) {

    //set a timer to turn off the indicator after a max of 15 seconds because 
    //  sometimes hideLoadingIndicator doesn't get called or there's some mixup 
    //  with who called it
    if (timeout) setTimeout(hideLoadingIndicator, timeout);

    var thisCaller = showLoadingIndicator.caller.name;

	var indicator = $('#loading-indicator').removeClass('hidden');
	//$('#loading-indicator-background').css('display', 'block');

    // check the .data() to see if any other functions called this
    indicator.data('callers', indicator.data('callers') === undefined ? 
    	[thisCaller] : indicator.data('callers').concat([thisCaller])
    )

}


function hideLoadingIndicator(caller) {
    

    var indicator = $('#loading-indicator')
    // if no caller was given, just remove the indicator
    if (caller === undefined) {
         indicator.data('callers', [])
    } else if (indicator.data('callers').includes(caller)) {
        indicator.data(
            'callers', 
            indicator.data('callers').filter(thisCaller => thisCaller != caller)
        );
    }

    // Hide the indicator if there are no more callers
    if (!indicator.data('callers').length) {
        //$('#loading-indicator-background').addClass('hidden');
        indicator.addClass('hidden');
    }

}


function fillSelectOptions(selector, queryString, dbname, optionClassName='track-info-option') {
    

    var deferred = $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'query', dbname: dbname, queryString: queryString},
        cache: false,
        success: function(queryResultString){
            var queryResult = queryResultString.startsWith('ERROR') ? false : $.parseJSON(queryResultString);
            if (queryResult) {
                queryResult.forEach(function(object) {
                    $(selector).append(
                        `<option class="${optionClassName}" value="${object.value}">${object.name === undefined ? object.value : object.name}</option>`
                    );
                })
            } else {
                console.log(`error filling in ${selectElementID}: ${queryResultString}`);
            }
        },
        error: function(xhr, status, error) {
            console.log(`fill select failed with status ${status} because ${error} from query:\n${sql}`)
        }
    });

    return deferred;
}

function hideVertices(id) {
	var fileName = getSelectedFileName();//currentFile//$('.collapse.show').text()
	var pointLayer = pointGeojsonLayers[fileName][id];
	//$(pointLayer._path).removeClass('cut-cursor-enabled');
	map.removeLayer(pointLayer); 

	//unselect this legend item (if it's selected)
	$('#legend-' + fileName + '-' + id)
		.removeClass('legend-row-selected')
		.addClass('legend-row')
		.children()
			.removeClass('legend-cell-selected')
			.addClass('legend-cell');
}


function getLineID(layer) {
	var fileName = getSelectedFileName();//currentFile//$('.collapse.show').text()
	for (var segmentID in lineLayers[fileName]) {
		if (lineLayers[fileName][segmentID] == layer) {
			return segmentID;
		}
	}
}


function selectLegendItem(id) {
	/* 
	Make item look selected in the legend
	*/

	var fileName = getSelectedFileName()
	
	// if there was no selected file, loop through and look for the first visible line
	if (id < 0) {
		for (segmentID in trackInfo[fileName]) {
			if (trackInfo[fileName][segmentID].visible) {
				id = segmentID;
				selectedLines[fileName] = segmentID;
				$('#img-zoom_selected').parent().removeClass('leaflet-toolbar-icon-disabled');
				break;
			}
		}

		// If none were selected, just select the first one
		if (id < 0) {
			id = Object.keys(trackInfo[fileName])[0];
			selectedLines[fileName] = id;
			$('#img-zoom_selected').parent().removeClass('leaflet-toolbar-icon-disabled');
		}
	}

	// Deselect all currently selected .legend-cell items,
	//  which actually contain the formatting
	$('.legend-cell-selected')
		.removeClass('legend-cell-selected')
		.addClass('legend-cell');
	
	// Remove the selection class from the row
	$('.legend-row-selected')
		.removeClass('legend-row-selected')
		.addClass('legend-row')

	// The row was a assigned the id, so get it's children (the .legned-cells)
	//  and add the class that contains the 
	$(`#legend-${fileName}-${id}`)
		.removeClass('legend-row')
		.addClass('legend-row-selected')
		.children()
		.removeClass('legend-cell')
		.addClass('legend-cell-selected');

	// If the checkbox was unselected, select it
	// This only works if legend-row events are filtered and only allowed 
	//  to propogate if the event didn't originate on a checkbox
	var thisCheckbox = $(`#legend-checkmark-${fileName}-${id}`);
	if (!thisCheckbox.prop('checked') && lineLayers[fileName][id] != undefined) {
		thisCheckbox.prop('checked', true);
		lineLayers[fileName][id].addTo(map);
	}

	var scrollPosition = $(`#legend-${fileName}`).scrollTop();
	var rowHeight = parseInt($(`#legend-${fileName}-${id}`).css('height').replace('px', ''));
	var legendHeight = parseInt($(`#legend-${fileName}`).css('height').replace('px', ''));
	let elementIndex = $(`#legend-${fileName}-${id}`).index();
	var scrollTo = elementIndex * rowHeight - rowHeight;

	// scroll to the row if it's off the screen
	if (scrollTo < scrollPosition || scrollTo > scrollPosition + legendHeight - rowHeight) {
		$(`#legend-${fileName}`)
			.animate(
				{scrollTop: scrollTo < 0 ? 0 : scrollTo}, 
				300
			);
	}


}


function onLineClick(e) {
	/*
	When a line is clicked, hide the vertices of the active 
	line and show this line's vertices
	*/

	if (!e.originalEvent.ctrlKey) {
		var thisLineID = getLineID(e.target);

		showVertices(thisLineID);

		selectLegendItem(thisLineID);

		
		L.DomEvent.stop(e) // don't propagate to the map
	}
}


function showNoFileMessage() {
	
	$('#no-files-message').removeClass('hidden');
	$('#no-files-gif').attr('src', noFileGIFS[Math.floor(Math.random() * noFileGIFS.length)]);
}


async function removeFile(fileName) {

    var nextCard = $(`#card-${fileName}`).next();
    // check if there is a next card. If not, try to get the previous card
    nextCard = nextCard.length ? nextCard : $(`#card-${fileName}`).prev()

    $(`#card-${fileName}`)
        .fadeOut(500, function() {$(this).remove()});// remove the item from the legend

   	if (nextCard.length) {
	    var nextFileName = nextCard[0].id.replace('card-', '');
	    var nextFilePath = `data/${nextFileName}_geojsons.json`;

	    fileWasSelected(nextFileName);
	    if (pointGeojsonLayers[nextFileName] == undefined) {
			loadTracksFromJSON(nextFilePath)
				.done(() => {
					hideLoadingIndicator('loadTracksFromJSON')
					// Reset zoom buffer
					//mapExtentBuffer = [map.getBounds()]
					//currentMapExtentIndex = 0;
				});
	    } else {
	        loadTracksFromMemory(nextFileName)
	        	.then(() => {
					hideLoadingIndicator('loadTracksFromMemory')
					// Reset zoom buffer
					//mapExtentBuffer = [map.getBounds()]
					//currentMapExtentIndex = 0;
				});
	    }
	} else {
		showLoadingIndicator(timeout=false);
		setTimeout(() => {
			hideLoadingIndicator();
			showNoFileMessage();
		}, 1000);
	}

    // send ajax to delete file
    var oldFilePath = `data/${fileName}_geojsons.json`;
    $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'deleteFile', filePath: oldFilePath},
        cache: false,
        success: function(success){
            if (!success) {
                alert(`problem deleting file ${oldFilePath}. This file will have to be manually deleted.`)
            }
        }
    });//*/

    delete isEditing[fileName];

}


function deleteTrack(id=undefined, showAlert=true, isRedo=false) {
	/*
	Remove the currently selected line and points from the 
	map and delete references to them
	*/

	var fileName = getSelectedFileName();
	var thisID = id === undefined ? selectedLines[fileName] : id;
	if (thisID >= 0 && (showAlert ? confirm(`Are you sure you want to delete the selected track?`) : true)) {
        if (Object.keys(pointGeojsonLayers[fileName]).length === 1) {
            var deleteFile = confirm('This is the only track left in this file. Deleting it will delete the file. Are you sure you want to continue?');
            if (deleteFile) {
                removeFile(fileName);
            } else {
                return;
            }
        }
		map.removeLayer(pointGeojsonLayers[fileName][thisID]);
		map.removeLayer(lineLayers[fileName][thisID]);

		// If the user is not redoing the edit action (i.e., pressed ctr + shift + Z), 
		//	reset the buffer because this is a new action
		if (!isRedo) {
			redoBuffer = [];
			editingBufferIndex ++;
			undoBuffer[editingBufferIndex] = {
				function: undoDeleteTrack,
				args: {
					fileName: fileName,
					pointGeoJSON: pointGeojsonLayers[fileName][thisID].toGeoJSON(),
					latlngs: lineLayers[fileName][thisID].getLatLngs(),
					thisTrackInfo: JSON.parse(JSON.stringify(trackInfo[fileName][thisID])),
					segmentID: thisID
				}
			}
			toggleUndoButton();
		}

		// Delete items and remove from legend
		delete pointGeojsonLayers[fileName][thisID];
		delete lineLayers[fileName][thisID];
		delete trackInfo[fileName][thisID];
		$(`#legend-${fileName}-${thisID}`)
			.fadeOut(500, function() {$(this).remove()});// remove the item from the legend
		
		// Only reset the selected line if this line is the currently selected one
		if (thisID == selectedLines[fileName]) {
			selectedLines[fileName] = -1;
			$('#img-zoom_selected').parent().addClass('leaflet-toolbar-icon-disabled');
		}

		isEditing[fileName] = true;
	} 
	//updateLegend();

}


function undoDeleteTrack({fileName, pointGeoJSON, latlngs, thisTrackInfo, segmentID}) {

	let thisID = trackInfo[fileName][segmentID] === undefined ?
		segmentID : // the ID doesn't already exist
		Math.max.apply(null, Object.keys(lineLayers[fileName])) + 1; // it does so just get the next available ID

	// recreate the layers (for some reason, references to leaflet objects don't seem to work)
	colors[fileName][segmentID] = colors[fileName][segmentID] === undefined ? getColor() : colors[fileName][segmentID];
	pointGeojsonLayers[fileName][segmentID] = L.geoJSON(pointGeoJSON, { 
		onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
		pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, colors[fileName][segmentID])),
		style: {className: 'cut-cursor-eligible'}
	});
	lineLayers[fileName][segmentID] = L.polyline(
		latlngs, 
		options={color: colors[fileName][segmentID]})
	.addEventListener({click: onLineClick});
	
	trackInfo[fileName][segmentID] = thisTrackInfo;
	
	if (fileName !== getSelectedFileName()) {
		fileWasSelected(fileName);
		loadTracksFromMemory(fileName)
	    	.then(() => {hideLoadingIndicator('loadTracksFromMemory')});
	    
	    if (selectedLines[fileName] !== segmentID) {
			hideVertices(selectedLines[fileName]);
			selectedLines[fileName] = segmentID;
			$('#img-zoom_selected').parent().removeClass('leaflet-toolbar-icon-disabled');
		}
	}

	// Show the recreated line as selected
	showVertices(segmentID);
	updateLegend(fileName);
	selectLegendItem(segmentID);

	redoBuffer[editingBufferIndex - 1] = {
		function: redoDeleteTrack,
		args: {
			fileName: fileName,
			segmentID: segmentID
		}
	}
}


function redoDeleteTrack({fileName, segmentID}) {

	if (fileName !== getSelectedFileName()) {
		fileWasSelected(fileName);
		loadTracksFromMemory(fileName)
	    	.then(() => {hideLoadingIndicator('loadTracksFromMemory')});
	}

	deleteTrack(segmentID, showAlert=false, isRedo=true);

}


function toggleUndoButton() {
	
	if (editingBufferIndex >= 1 ) {
		$('#img-undo').parent().removeClass('leaflet-toolbar-icon-disabled');//.css('opacity', editingBufferIndex < redoBuffer.length ? 1 : 0.35)
	} else {
		$('#img-undo').parent().addClass('leaflet-toolbar-icon-disabled');
	}

	if (redoBuffer.length > 0) {
		$('#img-redo').parent().removeClass('leaflet-toolbar-icon-disabled');//.css('opacity', editingBufferIndex < redoBuffer.length ? 1 : 0.35)
	} else {
		$('#img-redo').parent().addClass('leaflet-toolbar-icon-disabled');
	}

}


function undoMapEdit() {

	let editAction = undoBuffer[editingBufferIndex];
	editAction.function(editAction.args);
	editingBufferIndex --;

	$('#img-redo').parent().removeClass('leaflet-toolbar-icon-disabled');
	toggleUndoButton();
}


function redoMapEdit() {
	
	let editAction = redoBuffer[editingBufferIndex];
	editAction.function(editAction.args);
	editingBufferIndex ++;

	$('#img-undo').parent().removeClass('leaflet-toolbar-icon-disabled');//.css('opacity', 1);
	if (editingBufferIndex < redoBuffer.length) {
		$('#img-redo').parent().removeClass('leaflet-toolbar-icon-disabled');//.css('opacity', editingBufferIndex < redoBuffer.length ? 1 : 0.35)
	} else {
		$('#img-redo').parent().addClass('leaflet-toolbar-icon-disabled');
	}
}


function undoButtonClick() {

	// If the button is disabled, exit
	if ($('#img-undo').parent().hasClass('leaflet-toolbar-icon-disabled')) return;

	undoMapEdit();
}

function redoButtonClick() {

	// If the button is disabled, exit
	if ($('#img-redo').parent().hasClass('leaflet-toolbar-icon-disabled')) return;

	redoMapEdit();
}


function onKeyDown(e) {

	// User pressed delete key -> delete the selected track
	if (e.key === 'Delete') {
		var fileName = getSelectedFileName();
		if (selectedLines[fileName] === undefined || selectedLines[fileName] < 0) {
			alert(`No track is currently selected. Click a track first then press 'Delete' or click a delete button in the legend`);
		} else {
			deleteTrack();
		}
	// User pressed ctl + z -> undo the last map edit
	} else if (e.key === 'z' && e.ctrlKey && !e.shiftKey) {
		if (editingBufferIndex < 1) {
			alert('No actions to undo');
		} else {
			undoMapEdit();
		}
	// User pressed ctl + shift + z -> redo the last map edit
	} else if (e.key === 'Z' && e.ctrlKey && e.shiftKey) {
		if (editingBufferIndex >= redoBuffer.length) {
			alert('No actions to redo');
		} else {
			redoMapEdit();
		}
	}
}


function onMapClick(e) {
	/*
	If the map is clicked (not a polyline layer), deselect the current layer.
	Also, remove the cut-cursor-enabled class in case it was added
	*/

	var fileName = getSelectedFileName()
	var currentLineID = selectedLines[fileName]
	if (currentLineID >= 0 && !e.originalEvent.ctrlKey) {
		hideVertices(currentLineID);
		selectedLines[fileName] = -1;
		$('#img-zoom_selected').parent().addClass('leaflet-toolbar-icon-disabled');
	}

	// If the split tool was selected, deselect and don't show the cut icon on hover
	$('.cut-cursor-enabled').removeClass('cut-cursor-enabled');
	$('#img-split_vertex').parent().removeClass('map-tool-selected');
}


function addFileToMenu(filePath) {
	 
	var fileName = filePath.replace('data/', '').replace('_geojsons.json', '');
	//$('<tr><td class="file-list-row">' + fileName + '</td></tr>')
	//.appendTo('#files-table')
	var cardID = 'card-' + fileName;
	var cardHeaderID = 'cardHeader-' + fileName;
	var contentID = 'cardContent-' + fileName;
	// add the card
	var $card = $(
		'<div class="card" id="' + cardID + '">' + 
			'<div class="card-header px-0" id="' + cardHeaderID + '" style="width:100%;"></div>' +
		'</div>'
	).appendTo('#file-list');

	 // add an anchor to the card header
	$(
	`<div class="row mx-0" style="width:100%; display: flex; flex-direction: column; height: 40px;">
	 	<a class="collapsed card-link" data-toggle="collapse" href="#${contentID}" style="display:inline-block; flex: 1; min-width:50%; max-width:75%; padding-left:5%;">
			<p class="card-title">${fileName}</p>
		</a>
		<div style="display:inline-block; float:right; width: 100px;">
			<button class="file-card-button delete" title="Delete file" id="delete-${fileName}"></button>
			<button class="file-card-button import" title="Import tracks from file" id="import-${fileName}"></button>
		</div>
	</div>`
	)
	.appendTo('#' + cardHeaderID)
	.find('a').on('click', function(event) {
		if (!$(this).closest('.card').hasClass('selected')){
			
			// Make sure the new card is given the selected class before trying to load data
			fileWasSelected(fileName);
			
			// If the points don't yet exist, this file hasn't been loaded, so load them
			if (pointGeojsonLayers[fileName] == undefined) {
				loadTracksFromJSON(filePath)
					.done(() =>  {hideLoadingIndicator('loadTracksFromJSON')});
		    } else {
		        loadTracksFromMemory(fileName)
		        	.then(() => {hideLoadingIndicator('loadTracksFromMemory')});
		    }
		}
	 });

	// Add event handlers that prevent propogation to the card
	$(`#delete-${fileName}`).click((event) => {
		if (confirm(`Are you sure you want to delete "${fileName}"?`)) {
			removeFile(fileName);
		}
		event.stopPropagation();
	})
	$(`#import-${fileName}`).click((event) => {
		onImportDataClick(fileName);
		event.stopPropagation();
	})

	// Add the card content to the card
	const cardFooterHTML = `
    <div class="track-info-panel dark-scrollbar" id="track-info-panel-${fileName}">
      <!-- Info about each track-->
      <div class="row track-info-header-container">
        <div class="track-info-header-title-container">
          <h5>Track info</h5>
          <button class="button-track-info-lock" id="button-track-info-lock-${fileName}" title="Toggle lock on track info" onclick="lockButtonClick('${fileName}')"></button>
        </div>
        <div class="submission-info-container" style="">
          <p class="submission-info" id="p-submitted-at-${fileName}"> </p>
          <p class="submission-info" id="p-submitted-by-${fileName}"> </p>
        </div>
      </div>

      <form class="track-info-form" id="form-track-info-${fileName}">
        
        <div class="row">
          <div class="input-container">
            <label class="track-info-label">Tail number</label>
            <input type="text" class="track-info-textbox locked" id="textbox-registration-${fileName}" name="registration" spellcheck="false" disabled="true">
          </div>
          <div class="input-container">
            <label class="track-info-label">Editor</label>
            <input type="text" class="track-info-textbox locked track-editor" id="textbox-track_editor-${fileName}" name="editor" spellcheck="false" disabled="true">
          </div>
        </div>
        
        <div class="row">
          <div class="input-container">
            <label class="track-info-label">Operator code</label>
            <select class="track-info-textbox locked operator-code" id="select-operator_code-${fileName}" name="operator_code" disabled="true">
              <option class="track-info-option" value=""></option><!--make first option blank-->
            </select>
          </div>
          <div class="input-container">
            <label class="track-info-label" id="label-nps_mission_code-${fileName}">NPS mission code</label>
            <select class="track-info-textbox locked nps-mission-code" id="select-nps_mission_code-${fileName}" name="mission_code" disabled="true">
              <option class="track-info-option" value=""></option>
            </select>
          </div>
        </div>

        <div class="row">
          <div class="input-container input-container-large input-container-submitter-notes">
            <label class="track-info-label">Submitter notes</label>
            <div class="track-info-textbox locked" id="div-submitter_notes-${fileName}">
              <p id="p-submitter_notes-${fileName}"></p>
            </div>
          </div>
        </div>

      </form>
    </div>
	`;
	$('<div id="' + contentID + '" class="collapse" aria-labeledby="' + cardHeaderID + '" data-parent="#file-list">' + 
			'<div class="card-body p-0" id="' + contentID + '-body">' +
				'<div class="dark-scrollbar" id="legend-' + fileName + '" style="display:block; width:100%; overflow:auto; max-height:250px;"></div>' +
			'</div>' +
			'<div class="card-footer">' + cardFooterHTML + '</div>' +
		'</div>'
		).appendTo('#' + cardID);

	isEditing[fileName] = false;


}


function updateLegend(fileName){

	var legendID = 'legend-' + fileName;
	$('#' + legendID).empty();
	
	// Add a new row for each track
	for (segmentID in trackInfo[fileName]) {
		var thisInfo = trackInfo[fileName][segmentID];
		var thisColor = colors[fileName][segmentID];
		var htmlString = 
		`
		<div class="legend-row" id="legend-${fileName}-${segmentID}" style="width: 100%;">
			<div class="legend-cell px-10" style="width:60px; text-align:right;">
				<label class="legend-patch" for="colorPicker-${fileName}-${segmentID}">
					<svg height="10" width="60">
						<line x1="0" y1="5" x2="50" y2="5" style="stroke:${thisColor}; stroke-width:3;"></line>
					</svg>
				</label>
				<input class="line-color-picker" type="color" id="colorPicker-${fileName}-${segmentID}" name="colorPicker-${fileName}-${segmentID}" value="${thisColor}">
			</div>
			<div class="legend-cell" style="width:40%; max-width: 60%; text-align:left;">${thisInfo.departure_datetime}</div>
			<div class="legend-cell" style="max-width:10%; width:10%; margin-left:10px; margin-right:10px;">
				<button class="delete-track-button" title="Delete track" onclick="deleteTrack(${segmentID})"></button>
			</div>
			<div class="legend-cell" style="text-align:center; max-width:10%; width:10%; padding-right:10px; padding-left:10px;">
				<label class="checkmark-container">
					<input type="checkbox" checked="checked" id="legend-checkmark-${fileName}-${segmentID}">
					<span class="checkmark"></span>
				</label>
			</div>
		</div>
		`;
		var legendItem = $(htmlString)
			.click(function(event) {
				// If the event originated on a checkbox, exit
				if ($(event.target).hasClass('checkmark') || $(event.target).attr('checked') || $(event.target).hasClass('delete-track-button')) {
					return;
				}


				var thisID = $(this)[0].id.replace('legend-' + fileName + '-', '');
				showVertices(thisID);
				selectLegendItem(thisID);
			})
			.appendTo('#' + legendID);

		// Handle clicks on this item's checkbox
		var checkbox = $(`#legend-checkmark-${fileName}-${segmentID}`).change(function(event) {

			var thisID = parseInt($(this)[0].id.replace(`legend-checkmark-${fileName}-` , ''));
			if (!this.checked) {// state is after click
				if (thisID == selectedLines[fileName]) {
					hideVertices(thisID);
				}
				map.removeLayer(lineLayers[fileName][thisID]);
				trackInfo[fileName][thisID]['visible'] = false;
				var thisFileName = getSelectedFileName();
				selectedLines[thisFileName] = selectedLines[thisFileName] == thisID ? -1 : selectedLines[thisFileName];
			} else {
				map.addLayer(lineLayers[fileName][thisID]);
				trackInfo[fileName][thisID]['visible'] = true;
			}

			// Make sure the click doesn't continue to the .legend-row and highlight it
			event.stopPropagation();
		})

		// Set the checbox as checked or not depending on if the line is visible
		checkbox.prop('checked', thisInfo.visible);

	}

	$('.line-color-picker').change(function() {
		var colorPicker = $(this);
		var color = colorPicker.val();
		var legendLine = colorPicker.parent().find('line');
		legendLine.css('stroke', color);

		const [fileName, segmentID] = colorPicker.attr('id').replace('colorPicker-', '').split('-');

		var geojsonPoints = pointGeojsonLayers[fileName][segmentID];
		geojsonPoints.eachLayer((layer) => {layer.setStyle({color: color, fillColor: color})});

		lineLayers[fileName][segmentID].setStyle({color: color});

		colors[fileName][segmentID] = color;
	})
	
	// select the row corresponding to the selected line
	selectLegendItem(selectedLines[fileName]);
}


function removeAllLayers() {
	
	map.eachLayer( function(layer) {
		if (!(layer instanceof L.TileLayer) && !(layer instanceof L.Toolbar2)) {
			map.removeLayer(layer);
		}
	})
}


function fileWasSelected(filePath) {

	var newFileName = filePath.replace('data/', '').replace('_geojsons.json', '');
	
	// remove selected class styling from the .card-header anchor
	var oldFileName = getSelectedFileName();//currentFile
	var $oldCard = $('#card-' + oldFileName).removeClass('selected');
	var $oldCollapse = $('#cardContent-' + oldFileName);
	var $newCard = $('#card-' + newFileName).addClass('selected');
	var $newCollapse = $('#cardContent-' + newFileName);
	
	
	// Fill track info if the trackInfo exists already. If it doesn't exist,
	//	it'll be filled later
	if (oldFileName != newFileName && trackInfo[newFileName] != undefined) {
		
		fillTrackInfo(newFileName);

		// Even though the oeprator_code select's value changes, for some reason 
		//  the onchange event isn't fired, so just do so manually
		$(`#select-operator_code-${newFileName}`).change();
		
	}

	// When the old collapse collapses, scroll to the new card
	if ($oldCollapse.hasClass('show')) {
		$oldCollapse.on('hidden.bs.collapse', function() {
			// Make sure the card takes up as much of the menu scrollview as possible
			$newCard[0].scrollIntoView({behavior: 'smooth', block: 'start'})
			if (!$newCollapse.hasClass('show')) $newCollapse.collapse('show');
			
			// Remove the event listener for this collapse so it doesn't continue to fire whenever the old collapse is 
			$oldCollapse.off('hidden.bs.collapse');
		}).collapse('hide');
	} else {
		// Make sure the card takes up as much of the menu scrollview as possible
		$newCard[0].scrollIntoView({behavior: 'smooth', block: 'start'})
		if (!$newCollapse.hasClass('show')) $newCollapse.collapse('show');
	}

}


function loadTracksFromJSON(filePath) {
	// Read the track JSON file and 

	// start the loading indicator
	showLoadingIndicator(timeout=false);

	removeAllLayers();

	var fileName = filePath.replace('data/', '').replace('_geojsons.json', '');
	
	// Initialize objects for this file
	pointGeojsonLayers[fileName] = {};
	lineLayers[fileName] = {};
	lineCoords[fileName] = {};
	colors[fileName] = {};
	minVertexIndices[fileName] = {};
	fileExtents[fileName] = {};
	trackInfo[fileName] = {};  

	var deferred = $.ajax({ // use .ajax() instead of getJSON to set cache=false
		url: filePath,
		dataType: 'json',
		cache: false,
		success: function(data) {
			/* data come in as
			{
				track_info: 
					{
						prop1: val1
						...
					},
				geojsons: 
					{
						0: geojson_data 
						...
					}
			}*/
			data['track_info']['visible'] = true;
			// Each item in the JSON file is a separate geojson 
			for (segmentID in data.geojsons) {
				var geojson = data.geojsons[segmentID];
				var firstProperties = geojson.features[0].properties;
				data.track_info['departure_datetime'] = firstProperties.departure_datetime;
				data.track_info['registration'] = firstProperties.registration;
				trackInfo[fileName][segmentID] = {...data.track_info};
				trackInfo[fileName][segmentID]['trackInfoUnlocked'] = false;
				
				var color = getColor();
				colors[fileName][segmentID] = color;
				pointGeojsonLayers[fileName][segmentID] = L.geoJSON(geojson, {
					onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
					pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, color)),
					style: {className: 'cut-cursor-eligible'}
				});

			}
		}
	}).done(function() {    
		// Once the async call is done, add all the lines and 
		//  the points for a single line to the map
		var polylineCoords = [];
		for (var segmentID in lineCoords[fileName]) {
			var line = L.polyline(
				lineCoords[fileName][segmentID], 
				options={color: colors[fileName][segmentID]}
				)
			.addTo(map);
			line.on({click: onLineClick});
			lineLayers[fileName][segmentID] = line;
			polylineCoords.push(lineCoords[fileName][segmentID]);
		}

		// Show first line
		if (selectedLines[fileName] == undefined) {
			selectedLines[fileName] = 0;
			$('#img-zoom_selected').parent().removeClass('leaflet-toolbar-icon-disabled');
		}

		var allLines = L.polyline(polylineCoords);
		fileExtents[fileName] = allLines.getBounds();
		showVertices(selectedLines[fileName]);

		// Reset zoom buffer
		mapExtentBuffer = []
		currentMapExtentIndex = -1;
		map.fitBounds(fileExtents[fileName]);

		// Add lines to legend
		updateLegend(fileName);

		// Fill in the rest of the track info (from reading the geojsons)
		fillTrackInfo(fileName);

	}).fail(function(d, textStatus, error) {
        alert(`failed to read track data for "${fileName}" because of the following error: ${error}`);
        hideLoadingIndicator('loadTracksFromJSON');
    })

	return deferred;
}


async function loadTracksFromMemory(fileName) {

	showLoadingIndicator(timeout=false);

	// remove current layers
	removeAllLayers();

	// Add the lines for this file
	for (segmentID in lineLayers[fileName]) {
		if (trackInfo[fileName][segmentID].visible) {
			lineLayers[fileName][segmentID].addTo(map);
		}
	}

	// show the right file as selected
	//fileWasSelected(fileName);

	// Update the legend
	updateLegend(fileName);

	//show points for the most recently selected segment
	if (selectedLines[fileName] >= 0) {
		showVertices(selectedLines[fileName], hideCurrent=false);
	}

	mapExtentBuffer = []
	currentMapExtentIndex = -1;

	// zoom to extent
	map.fitBounds(fileExtents[fileName])

	//hideLoadingIndicator('loadTracksFromMemory');

	// Reset zoom buffer
	//mapExtentBuffer = [map.getBounds()]
	//currentMapExtentIndex = 0;
}


function fillTrackInfo(fileName){

	//var fileName = getSelectedFileName();
	var currentSegmentID = selectedLines[fileName];
	var thisInfo = trackInfo[fileName][currentSegmentID];

	// If there's no info for the selected track (= -1), get the first 
	//  one because each track for a given file has the same global track info
	if (thisInfo == undefined) {
		for (segmentID in trackInfo[fileName]) {
			// get the first one
			thisInfo = trackInfo[fileName][segmentID];
			break;
		}
	}

	for (key in thisInfo) {
		//console.log(key + ': ' + thisInfo[key])
		$(`#textbox-${key}-${fileName}`).val(thisInfo[key]);
		$(`#p-${key}-${fileName}`).text(thisInfo[key])//for the submitter comments
		$(`#select-${key}-${fileName}`).val(thisInfo[key])//for the select dropdowns
	}

	if ($(`#select-operator_code-${fileName}`).val() !== 'NPS') {
		$(`#select-nps_mission_code-${fileName}`).addClass('select-disabled')
	}
	//$('#track-info-subtitle').text(`Submitted at ${thisInfo.submission_time}\nby ${thisInfo.submitter}`)
	$('#p-submitted-at-' + fileName).text(`Submitted at ${thisInfo.submission_time}`);
	$('#p-submitted-by-' + fileName).text(`by ${thisInfo.submitter}`);

	// Change the state of the lock button (and the form) if necessary
	var $lockButton = $(`#button-track-info-lock-${fileName}`);
	if ($lockButton.hasClass('unlocked') != thisInfo.trackInfoUnlocked) {
		lockButtonClick(fileName);	
	}
}


function onTrackInfoElementChange(target) {
    var fileName = getSelectedFileName();
    var thisValue = $(target).val();
    var propertyName = $(target).attr('id').replace('textbox-', '').replace('select-', '').replace(`-${fileName}`, '');
    for (segmentID in trackInfo[fileName]) {
        trackInfo[fileName][segmentID][propertyName] = thisValue; 
    }
}


function onOperatorChange(selectedOperator){
	// **** change to be compatible with info/file *** //

	//var selectedOperator = $(target).val();
	var fileName = getSelectedFileName();
	var selectedMission = $('#select-nps_mission_code-' + fileName).val();
	
	// Update track info
	if (trackInfo[fileName] !== undefined && selectedOperator != null) {
		for (segmentID in trackInfo[fileName]) {
			trackInfo[fileName][segmentID]['operator_code'] = //capture value
                selectedOperator.length ? selectedOperator : trackInfo[fileName][segmentID]['operator_code']; 
		}
	}

	// If the operator isn't the NPS, disable mission_code select
	if (selectedOperator !== 'NPS') {
		$('#label-nps_mission_code-' + fileName).addClass('select-disabled-label');
		// Record the currently selected mission code
		if (trackInfo[fileName] !== undefined) {
			for (segmentID in trackInfo[fileName]) {
				trackInfo[fileName][segmentID]['nps_mission_code'] = selectedMission; //capture value
			}
		}
		// Disable it and set the value as null
		$('#select-nps_mission_code-' + fileName)
			.addClass('select-disabled')
			.attr('disabled', 'true')
			.val('');
	// If it is NPS, make sure mission_code select is enabled
	} else {
		$('#label-nps_mission_code-' + fileName).removeClass('select-disabled-label');
		$('#select-nps_mission_code-' + fileName)
			.removeClass('select-disabled')
			.removeAttr('disabled');
		// Set the value to whatever was recorded before disabling
		var thisInfo = trackInfo[fileName];
		if (thisInfo !== undefined) {
			for (segmentID in thisInfo) {
				$('#select-nps_mission_code-' + fileName).val(thisInfo[segmentID]['nps_mission_code'].length ? thisInfo[segmentID]['nps_mission_code'] : '');
				break // just assign with the first one
			}
			
		}
	}

}


function lockButtonClick(fileName) {

	var lockButton = $('#button-track-info-lock-' + fileName);
	var infoForm = $('#form-track-info-' + fileName);
	if (lockButton.hasClass('unlocked')) {
		lockButton.removeClass('unlocked')
		infoForm.find('input[type="text"], select')
			.addClass('locked')
			.attr('disabled', 'true');
		/*infoForm.find('select')
			.addClass('locked')
			.attr('disabled', 'true');*/
	} else {
		lockButton.addClass('unlocked');
		infoForm.find('input[type="text"], select')
			.removeClass('locked')
			.removeAttr('disabled');
		/*infoForm.find('select').removeClass('locked')
			.removeClass('locked')
			.removeAttr('disabled');*/
	}

	// Check to see if mission code should be .disabled
	onOperatorChange($('#select-operator_code-' + fileName).val());

	if (trackInfo[fileName] !== undefined) {
		for (segmentID in trackInfo[fileName]) {
			trackInfo[fileName][segmentID]['trackInfoUnlocked'] = lockButton.hasClass('unlocked');
		}
	}
}


function onSplitButtonClick() {

	var thisTool = $('#img-split_vertex').parent();
	//thisTool.data('selected', thisTool.data('selected') ? false : true)
	thisTool.toggleClass('map-tool-selected');/*thisTool.hasClass('map-tool-selected') ? 
		thisTool.removeClass('map-tool-selected') :
		thisTool.addClass('map-tool-selected');*/

	$('.cut-cursor-enabled').length ?
		$('.cut-cursor-enabled').removeClass('cut-cursor-enabled') :
		$('.cut-cursor-eligible').addClass('cut-cursor-enabled');
}


function addMapToolbars() {
	/* Add a toolbar for zooming to full extent and the extent of the selected track*/

	// Map nav toolbar
	var zoomSelected = L.Toolbar2.Action.extend({
		options: {
			toolbarIcon: {
					html: '<img id="img-zoom_selected" src="imgs/zoom_selected_icon.svg"/>',
					tooltip: 'Zoom to selected track'
			}
		},
		addHooks: function () {
			if (!$('#img-zoom_selected').parent().hasClass('leaflet-toolbar-icon-disabled')) {
				var fileName = getSelectedFileName();
				if (fileName !== undefined) {
					map.fitBounds(lineLayers[fileName][selectedLines[fileName]].getBounds());
				} else {
					alert('The map is currently loading. Please wait to zoom until all tracks are loaded.');
				}
			}
		}
				
	});

	var zoomFull = L.Toolbar2.Action.extend({
		options: {
			toolbarIcon: {
					html: '<img id="img-zoom_full" src="imgs/zoom_full_icon.svg"/>',
					tooltip: 'Zoom to full extent'
			}
		},
		addHooks: function () {
			var fileName = getSelectedFileName();
			if (fileName !== undefined) {
				map.fitBounds(fileExtents[fileName]);
			} else {
				alert('The map is currently loading. Please wait to zoom until all tracks are loaded.');
			}
		}
	});

	var zoomPrevious = L.Toolbar2.Action.extend({
		
		options: {
			toolbarIcon: {
				html: '<img class="leaflet-toolbar-icon-img-disablable" id="img-zoom_previous" src="imgs/zoom_previous_icon.svg"/>',
				tooltip: 'Zoom to previous extent'
			}
		},
		addHooks: onPreviousExtentClick
	});

	var zoomNext = L.Toolbar2.Action.extend({
		
		options: {
			toolbarIcon: {
					html: '<img class="leaflet-toolbar-icon-img-disablable" id="img-zoom_next" src="imgs/zoom_next_icon.svg"/>',
					tooltip: 'Zoom to next extent'
			}
		},
		addHooks: onNextExtentClick
	});


	// Editing toolbar
	var cut = L.Toolbar2.Action.extend({
		
		options: {
			toolbarIcon: {
					html: '<img id="img-split_vertex" src="imgs/cut_icon_30px.svg"/>',
					tooltip: 'Split track at vertex'
			}
		},
		addHooks: onSplitButtonClick
	});

	var undo = L.Toolbar2.Action.extend({
		
		options: {
			toolbarIcon: {
					html: '<img class="leaflet-toolbar-icon-img-disablable" id="img-undo" src="imgs/undo_icon.svg"/>',
					tooltip: 'Undo map edit'
			}
		},
		addHooks: undoButtonClick
	});

	var redo = L.Toolbar2.Action.extend({
		
		options: {
			toolbarIcon: {
					html: '<img class="leaflet-toolbar-icon-img-disablable" id="img-redo" src="imgs/redo_icon.svg"/>',
					tooltip: 'Redo map edit'
			}
		},
		addHooks: redoButtonClick
	});
	new L.Toolbar2.Control({	
			actions: [zoomSelected, zoomFull, zoomPrevious, zoomNext, cut, undo, redo],
			position: 'topleft'
	}).addTo(map);

	// add disabled class to buttons that need it
	$('.leaflet-toolbar-icon-img-disablable').parent().addClass('leaflet-toolbar-icon-disabled');
}



function validateTrackInfo() {

	var fileName = getSelectedFileName()
    var trackInfoInputs = $('#form-track-info-' + fileName).find('.track-info-textbox, .track-info-textbox.locked').toArray();
    for (elementID in trackInfoInputs) {
        var thisElement = $(trackInfoInputs[elementID]);
        var thisLabel = $(thisElement.siblings()[0]).text();
        if (!(thisElement.is('input') || thisElement.is('select')) || thisElement.val() == undefined) continue;
        var thisID = thisElement[0].id;
        if (!(thisElement.val().length) && !thisElement.hasClass('select-disabled') && thisLabel !== "NPS mission code"){
            alert(`The "${thisLabel}" field is empty but all track info fields are mandatory.`);
            thisElement.focus();
            return false;
        }
    }

    var $registrationInput = $('#textbox-registration-' + fileName);
    if (!$registrationInput.val().match(/N\d{2,5}[A-Z]{0,2}/gi)) {
        alert(`The "Tail number" field entry isn't valid.`);
        $registrationInput.focus();
        return false;
    }

    var $missionCodeInput = $('#select-operator_code-' + fileName);
    if ($missionCodeInput.val() === 'NPS' && !$('#select-nps_mission_code-' + fileName).val().length){
        if (!confirm(`Are you sure you want to import this file without an NPS mission code selected?`)) {
	        $missionCodeInput.focus();
	        return false;
        }
    }

    return true;
}


function removeTemporaryImportFiles(filePath, trackInfoPath) {
    /* if importing data failed or was canceled, make sure the temp data are deleted*/
    $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: filePath}, cache:false});
    $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: trackInfoPath}, cache:false});
}


function importData(fileName){

    //var fileName = getSelectedFileName();
    var filePath = `data/edited/${fileName}.geojson`;
    var trackInfoPath = filePath.replace('.geojson', '_track_info.json')

    console.log(`python ..\\scripts\\import_from_editor.py ../web/${filePath} ../web/${filePath.replace('.geojson', '_track_info.json')} \\\\inpdenards\\overflights\\config\\poll_feature_service_params.json`)
    // send a post request to the PHP script to run the Python script
    var stderrPath = `errorLogs/${fileName}_${Date.now()}.log`;
    var data = {
        action: "importData",
        geojsonString: filePath,
        trackInfoString: trackInfoPath,
        stderrPath: stderrPath
    };

    
    $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: data,
        cache: false,
        success: function(importResponse) {
            $.ajax({
                url: 'geojson_io.php',
                method: 'POST',
                data: {action: 'readTextFile', textPath: data.stderrPath},
                cache: false,
                success: function(stderr) {
                    if (stderr.trim().length) {
                    	var error;
                    	var dbErrorDetail = stderr.match(/[\r\n]DETAIL.*[\r\n]/);
                    	if (dbErrorDetail != null) {
                    		error = dbErrorDetail.toString().replace('DETAIL: ', '').trim();
                    	} else {
	                        var lines = stderr.split('\n');
	                        var errorName = stderr.match(/[A-Z][a-z]*Error/)//standard python Exception ends in "Error" (e.g., ValueError);
	                        
	                        for (lineNumber in lines) {
	                            if (lines[lineNumber].startsWith(errorName)) {
	                                error = lines.slice(lineNumber).join('\n');//all remaining lines
	                                break;
	                            }
	                        }
	                    }

	                    var warnings = [...stderr.matchAll('UserWarning: .*')]
	                    	.map(s=>{ return s.toString().replace('UserWarning: ', '-')})
	                    	.join('\n')
	                    
	                    // Trim period at end of warnings or error
	                    error = error.endsWith('.') ? error.slice(0, error.length - 1) : error
	                    if (error) {
                        	alert(`An error occurred while trying to import the data: ${error}. If you can't resolve this issue yourself, please contact the overflight data steward at ${dataSteward}`);
                        	hideLoadingIndicator();
                    	} else if (warnings) {
                    		alert(`${importResponse.trim().replace(/\t/g, '')}\nHowever, the import operation produced the following warnings:\n${warnings}`.trim());
                    		hideLoadingIndicator();
                    	}

                    } else {
                        alert(importResponse.replace(/\t/g, ''));
                        
                        // Remove the file from the menu and delete it
                        removeFile(fileName);
                        hideLoadingIndicator();
                    }
                    // delete error log
                    //$.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: stderrPath},cache:false});
                }
            }).fail(
            	function(xhr, status, error) {
    				alert(`Unknown script status. Failed to read stderr file because of a ${status} error: ${error}. You can view this file yourself at ${stderrPath}`)                        
                	// make sure the file error log is deleted even if reading it failed
                	//$.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: stderrPath},cache:false});
            		 hideLoadingIndicator();
            	}
            );

            // Try to delete temp files. Don't worry about handling failures because it doesn't *really* matter if they don't get deleted
            removeTemporaryImportFiles(filePath, trackInfoPath)
            hideLoadingIndicator();

        }
    })
    .fail(
    	function(xhr, status, error) {
    		alert(`Failed to call import script because of a ${status} error: ${error}`)
        	removeTemporaryImportFiles(filePath, trackInfoPath)
    		hideLoadingIndicator();
    	}
    )

}

function onImportDataClick(fileName=undefined){

    if (!validateTrackInfo()) {
    	return;	
    } 

	var fileName = fileName === undefined ? getSelectedFileName() : fileName;
	var filePath = `data/edited/${fileName}.geojson`;
	
	// The point layes are in the format {segID: {geojson}} so just combine all of 
	//  the features into a single FeatureCollection GeoJSON object since the 
	//  segment IDs are also recorded in the properties of each feature
	var features = [];
	var hiddenTracks = [];
	for (segmentID in pointGeojsonLayers[fileName]) {
		var geojson = pointGeojsonLayers[fileName][segmentID].toGeoJSON();
        features = features.concat(geojson.features)
        if (!trackInfo[fileName][segmentID].visible) {
            hiddenTracks.push(segmentID);
        }
    }

    if (hiddenTracks.length) {
        var continueImporting = confirm(`There are ${hiddenTracks.length} track segments currently hidden, but all tracks listed for this file will be imported. Are you sure you want to continue?`)
        if (!continueImporting) {
        	//removeTemporaryImportFiles(filePath, trackInfoPath);
            return;
        }
    }

    // Create a proper geojson object for the import script to read
	var thisGeojson = {
		type: "FeatureCollection",
		crs: { type: "name", properties: { name: "urn:ogc:def:crs:OGC:1.3:CRS84" } },//wgs84
		features: features
	}

	// send a post request to the PHP script to write the geojson
	var data = {
		action: "writeFile",
		filePath: filePath,
		jsonString: JSON.stringify(thisGeojson)
	};

	var info = trackInfo[fileName][Object.keys(trackInfo[fileName])[0]];
	var thisTrackInfo = {};
	for (key in info) {
		var val = info[key];
		if (val != '') {
			thisTrackInfo[key] = val;
		}
	}
    //var thisTrackInfo = {...trackInfo[fileName][]}
    thisTrackInfo['track_editor'] = $('#textbox-track_editor-' + fileName).val()//set here because it might not have been
	
	showLoadingIndicator(timeout=false);
	$.when(
		$.ajax({
			url: 'geojson_io.php',
			method: 'POST',
			data: data,
	        cache: false
		}).fail(
			function(xhr, status, error) {
	            alert(`JSON track failed to save because of the following error: ${error}`);
	            hideLoadingIndicator();
	        }
	    ),
		$.ajax({
			url: 'geojson_io.php',
			method: 'POST',
            cache: false,
			data: {
				action: 'writeFile',
				filePath: filePath.replace('.geojson', '_track_info.json'),
				jsonString: JSON.stringify(thisTrackInfo)
			}
		}).fail(
        	function(xhr, status, error) {
            	alert(`JSON track failed to save because of the following error: ${error}`);
            	hideLoadingIndicator();
        	}
        )
	).then(
    	// With temporary files written, import the data
    	(geojsonDeferred, trackInfoDeferred) => {
    		if (geojsonDeferred[0] == false || trackInfoDeferred[0] == false) {
				alert('Unable to create temporary files for import. Check your network connection and try again.');
				hideLoadingIndicator();
    		} else {
    			importData(fileName);
    		}
    	}
    ); 

}