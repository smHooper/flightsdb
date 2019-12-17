
var fileIsLoading = false;
var dataSteward = 'dena_flight_data@nps.gov';

function getColor() {
	var color = Math.floor(Math.random() * 16777216).toString(16);
	var hexColor = '#000000'.slice(0, -color.length) + color;

	return hexColor;
}


function getSelectedFileName() {
	try {
		return $('.card-header-anchor-selected.card-link.text-center > .card-title').text();
	} catch {
		return '';
	}
}


function splitAtVertex(segmentID, vertexID, minVertexIndex){
	/*
	Split a line at the associated vertex. vertexID is the ID of the 
	point from the geojson used to create it
	*/

	var fileName = getSelectedFileName();//$('.collapse.show').text();
	var originalLine = lineLayers[fileName][segmentID];
	var allLatlngs = originalLine.getLatLngs();

	// vertexID is a global ID so calculate the index within the latlngs array
	var vertexIndex = vertexID - minVertexIndex 

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
	//  of the original and adding each .feature to the json
	var newDepartureTime = pointGeojsonLayers[fileName][segmentID].toGeoJSON().features[vertexIndex].properties.ak_datetime
	var newGeoJSON = {
		type: "FeatureCollection",
		features: []
	}
	pointGeojsonLayers[fileName][segmentID].eachLayer(
		function(layer) {
			var featureID = layer.feature.id;
			var theseCordinates = layer.feature.geometry.coordinates
			var theseProperties = layer.feature.properties
			// only add this point if it occurs after the splitting vertex
			//  (or if it is that vertex)
			if (featureID >= vertexID) {
				// set the mapID to the new segment ID so the points 
				//  are still related to the line
				theseProperties.mapID = newSegmentID;
				theseProperties.min_index = vertexID;
				layer.feature.properties.departure_datetime = newDepartureTime;
				// add this feature
				newGeoJSON.features.push(layer.feature);

				// if this feature isn't the splitting vertex, 
				//  drop it from the original point layer
				if (featureID > vertexID) {
					pointGeojsonLayers[fileName][segmentID].removeLayer(layer)
				};
			}
		}
	)
	pointGeojsonLayers[fileName][newSegmentID] = L.geoJSON(newGeoJSON, { 
		onEachFeature: ((feature, layer) => onEachPoint(feature, layer, fileName)), 
		pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, newColor))
	});

	// Update the legend
	var thisInfo = newGeoJSON.features[0].properties;
	thisInfo['visible'] = true;
	trackInfo[fileName][newSegmentID] = thisInfo;
	showVertices(newSegmentID);
	updateLegend(fileName);
}


function onPointClick(event, segmentID, vertexID, minVertexIndex) {
	if (event.originalEvent.ctrlKey) {
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
		click: (e => onPointClick(e, properties.mapID, properties.point_index, properties.min_index))
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
	
}


function hideVertices(id) {
	var fileName = getSelectedFileName();//currentFile//$('.collapse.show').text()
	map.removeLayer(pointGeojsonLayers[fileName][id]); 

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
	
	// if there was no selected file, loop through and look for the visible line
	if (id < 0) {
		for (segmentID in trackInfo[fileName]) {
			if (trackInfo[fileName][segmentID].visible) {
				id = segmentID;
				selectedLines[fileName] = segmentID;
				break;
			}
		}

		// If none were selected, just select the first one
		if (id < 0) {
			id = Object.keys(trackInfo[fileName])[0];
			selectedLines[fileName] = id;
		}
	}

	// Deselect all currently selected cell (<td>) items,
	//  which actually contain the formatting
	$('.legend-cell-selected')
		.removeClass('legend-cell-selected')
		.addClass('legend-cell');
	
	// Remove the selection class from the row (<tr>)
	$('.legend-row-selected')
		.removeClass('legend-row-selected')
		.addClass('legend-row')

	// The row was a assigned the id, so get it's children (the <td>s)
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
	var scrollTo = id * rowHeight;

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


function removeFile(fileName) {

    var nextCard = $(`#card-${fileName}`).next();
    $(`#card-${fileName}`)
        .fadeOut(500, function() {$(this).remove()});// remove the item from the legend

    var nextFileName = nextCard[0].id.replace('card-', '');

    fileWasSelected(nextFileName);
    if (pointGeojsonLayers[nextFileName] == undefined) {
        loadTracksFromJSON(`data/${nextFileName}_geojsons.json`);
    } else {
        loadTracksFromMemory(nextFileName);
    }

    //############# handle situations when there is no .next()##############3

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
    });

}


function deleteTrack(id=undefined) {
	/*
	Remove the currently selected line and points from the 
	map and delete references to them
	*/

	var fileName = getSelectedFileName();
	var thisID = id === undefined ? selectedLines[fileName] : id;
	if (thisID >= 0 && confirm(`Are you sure you want to delete the selected track?`)) {
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
		delete pointGeojsonLayers[fileName][thisID];
		delete lineLayers[fileName][thisID];
		delete trackInfo[fileName][thisID];
		$(`#legend-${fileName}-${thisID}`)
			.fadeOut(500, function() {$(this).remove()});// remove the item from the legend
		
		// Only reset the selected line if this line is the currently selected one
		if (thisID == selectedLines[fileName]) {
			selectedLines[fileName] = -1;
		}
	} 
	//updateLegend();

}


function onKeyDown(e) {

	if (e.key === 'Delete') {
		var fileName = getSelectedFileName();
		if (selectedLines[fileName] === undefined || selectedLines[fileName] < 0) {
			alert(`No track is currently selected. Click a track first then press 
				'Delete' or click a delete button in the legend`)
		} else {
			deleteTrack();
		}
	}
}


function onMapClick(e) {
	/*
	If the map is clicked (not a polyline layer), deselect the current layer
	*/

	var fileName = getSelectedFileName()
	var currentLineID = selectedLines[fileName]
	if (currentLineID >= 0 && !e.originalEvent.ctrlKey) {
		hideVertices(currentLineID);
		selectedLines[fileName] = -1;
	}
}


function addFileToMenu(filePath) {
	 
	 var fileName = filePath.replace('data/', '').replace('_geojsons.json', '');
	 //$('<tr><td class="file-list-row">' + fileName + '</td></tr>')
	 //.appendTo('#files-table')
	 var cardID = 'card-' + fileName;
	 var cardHeaderID = 'cardHeader-' + fileName;
	 var contentID = 'cardContent-' + fileName;
	 // add the card
	 $('<div class="card" id="' + cardID + '">' + 
				'<div class="card-header px-0" id="' + cardHeaderID + '"></div>' +
			'</div>'
		).appendTo('#file-list');

	 // add an anchor to the card header
	 $(`<a class="collapsed card-link text-center" data-toggle="collapse" href="#${contentID}">
				<p class="card-title">${fileName}</p>
			</a>`)
	 .appendTo('#' + cardHeaderID)
	 .on('click', function() {
		if (!$(this).hasClass('card-header-anchor-selected')){
			
			// Make sure the new card is given the selected class before trying to load data
			fileWasSelected(fileName);
			
			// If the points don't yet exist, this file hasn't been loaded, so load them
			if (pointGeojsonLayers[fileName] == undefined) {
				loadTracksFromJSON(filePath);
			} else {
				loadTracksFromMemory(fileName);
			}
		}
	 });

	 // Add the card content to the card
	 $('<div id="' + contentID + '" class="collapse" aria-labeledby="' + cardHeaderID + '" data-parent="#file-list">' + 
			'<div class="card-body p-0" id="' + contentID + '-body">' +
				'<div class="dark-scrollbar" id="legend-' + fileName + '" style="display:block; width:100%; overflow:auto; max-height:250px;"></div>' +
			'</div>' +
			'<div class="card-footer"></div>' +
		'</div>'
		).appendTo('#' + cardID);

}


function updateLegend(fileName){

	var legendID = 'legend-' + fileName;
	$('#' + legendID).empty();
	
	// Add a new row for each track
	for (segmentID in trackInfo[fileName]) {
		/*// If the element already exists, skip it
		if ($(`#legend-${fileName}-${segmentID}`).length) {
			continue;
		}*/
		var thisInfo = trackInfo[fileName][segmentID];
		var thisColor = colors[fileName][segmentID];
		var htmlString = 
		`
		<div class="legend-row" id="legend-${fileName}-${segmentID}" style="width: 100%;">
			<div class="legend-cell px-10" style="width:60px; text-align:right;">
				<svg height="10" width="60">
					<line x1="0" y1="5" x2="50" y2="5" style="stroke:${thisColor}; stroke-width:3;"></line>
				</svg>
			</div>
			<div class="legend-cell" style="width:40%; max-width: 60%; text-align:left;">${thisInfo.departure_datetime}</div>
			<div class="legend-cell" style="max-width:10%; width:10%; margin-left:10px; margin-right:10px;">
				<button class="delete-button" onclick="deleteTrack(${segmentID})"></button>
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
				if ($(event.target).hasClass('checkmark') || $(event.target).attr('checked') || $(event.target).hasClass('delete-button')) {
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
	
	// select the row corresponding to the selected line
	selectLegendItem(selectedLines[fileName]);
	//console.log($('#legend_0'))
}


function removeAllLayers() {
	
	map.eachLayer( function(layer) {
		if (!(layer instanceof L.TileLayer) && !(layer instanceof L.Toolbar2)) {
			map.removeLayer(layer);
		}
	})
}


function fileWasSelected(filePath) {

	// remove selection if this is not the currently selected file.
	//  otherwise, add -selected style
	$('.collapse.show').collapse('hide')

	var fileName = filePath.replace('data/', '').replace('_geojsons.json', '');
	
	// remove selected class styling from the .card-header anchor
	var oldFileName = getSelectedFileName();//currentFile
	$('#card-' + oldFileName)
		.css('background-color', 'rgb(75, 75, 75')
			.find('a')
				//.css('color', 'rgb(125, 125, 125)')
				.removeClass('card-header-anchor-selected')
				/*.hover(function() {
					$(this).css('color', 'rgb(125, 125, 125)')
				});*/

	$('.collapse').each(function(){
		var thisFileName = $(this)[0].id.replace('cardContent-', '');
		if (thisFileName == fileName) {
			$(this).collapse('show');

			// Style the card to look selected
			$('#card-' + thisFileName)
				.css('background-color', 'rgb(95, 95, 95)')
				.find('a')
					//.css('color', 'rgb(150, 150, 150)')
					.addClass('card-header-anchor-selected');
				
				if (oldFileName != thisFileName && trackInfo[thisFileName] != undefined) {
					fillTrackInfo();

					// Even though the oeprator_code select's value changes, for some reason 
					//  the onchange event isn't fired, so just do so manually
					$('#select-operator_code').change();
					
				}
		}
	});

}


function showLoadingIndicator() {

    //set a timer to turn off the indicator after a max of 5 seconds because 
    //  sometimes hideLoadingIndicator doesn't get called or there's some mixup 
    //  with who called it
    setTimeout(hideLoadingIndicator, 5000);

    var thisCaller = showLoadingIndicator.caller.name;

	var indicator = $('#loading-indicator').css('display', 'block')
	$('#loading-indicator-background').css('display', 'block');

    // check the .data() to see if any other functions called this
    if (indicator.data('callers') === undefined) {
        // If it's not defined, this is the only caller so set the value
        //   to an array to new callers can be added
        indicator.data('callers', [thisCaller])
    } else {
        // If it does exist, append this caller to the existing
        indicator.data('callers', indicator.data('callers').concat([thisCaller]))
    }

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
        $('#loading-indicator-background').css('display', 'none');
        indicator.css('display', 'none');
    }

}


function loadTracksFromJSON(filePath) {
	// Read the track JSON file and 

	// start the loading indicator
	fileIsLoading = true;
	showLoadingIndicator();

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
					pointToLayer: ((feature, latlng) => geojsonPointAsCircle(feature, latlng, color))
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
		}

		var allLines = L.polyline(polylineCoords);
		fileExtents[fileName] = allLines.getBounds();
		showVertices(selectedLines[fileName]);
		//map.fitBounds(fileExtents[fileName]);

		// Add lines to legend
		updateLegend(fileName);

		// Fill in the rest of the track info (from reading the geojsons)
		fillTrackInfo();

		// remove the loading indicator
		hideLoadingIndicator('loadTracksFromJSON');
		
	})

	return deferred;
}


function loadTracksFromMemory(fileName) {
	
	showLoadingIndicator();

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

	// zoom to extent
	map.fitBounds(fileExtents[fileName])

	hideLoadingIndicator('loadTracksFromMemory');
}


function fillTrackInfo(){

	var fileName = getSelectedFileName();
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
		$('#textbox-' + key).val(thisInfo[key]);
		$('#p-' + key).text(thisInfo[key])//for the submitter comments
		$('#select-' + key).val(thisInfo[key])//for the select dropdowns
	}
	//$('#track-info-subtitle').text(`Submitted at ${thisInfo.submission_time}\nby ${thisInfo.submitter}`)
	$('#p-submitted-at').text(`Submitted at ${thisInfo.submission_time}`);
	$('#p-submitted-by').text(`by ${thisInfo.submitter}`);

	// Change the state of the lock button (and the form) if necessary
	if ($('#button-track-info-lock').hasClass('unlocked') != thisInfo.trackInfoUnlocked) {
		lockButtonClick();	
	}
}


function onTrackInfoElementChange(target) {
    var fileName = getSelectedFileName();
    var thisValue = $(target).val();
    var propertyName = $(target)[0].id.replace('textbox-', '').replace('select-', '');
    for (segmentID in trackInfo[fileName]) {
        trackInfo[fileName][segmentID][propertyName] = thisValue; 
    }
}


function fillSelectOptions(selectElementID, queryString, columnName) {
	
    showLoadingIndicator();

	var deferred = $.ajax({
		url: 'geojson_io.php',
		method: 'POST',
		data: {action: 'query', queryString: queryString},
		cache: false,
		success: function(queryResultString){
			var queryResult = queryResultString.startsWith('ERROR') ? false : $.parseJSON(queryResultString);
			if (queryResult) {
				queryResult.forEach(function(object) {
					$('#' + selectElementID).append(
						`<option class="track-info-option" value="${object.value}">${object.value}</option>`
					);
				})
			} else {
				console.log(`error filling in ${selectElementID}: ${queryResultString}`);
			}
		}
	});

	return deferred;
}


function onOperatorChange(selectedOperator){

	//var selectedOperator = $(target).val();
	var selectedMission = $('#select-nps_mission_code').val();
	var fileName = getSelectedFileName();

	// Update track info
	if (trackInfo[fileName] !== undefined) {
		for (segmentID in trackInfo[fileName]) {
			trackInfo[fileName][segmentID]['operator_code'] = //capture value
                selectedOperator.length ? selectedOperator : trackInfo[fileName][segmentID]['operator_code']; 
		}
	}

	// If the operator isn't the NPS, disable mission_code select
	if (selectedOperator !== 'National Park Service') {
		$('#label-nps_mission_code').addClass('select-disabled-label');
		// Record the currently selected mission code
		if (trackInfo[fileName] !== undefined) {
			for (segmentID in trackInfo[fileName]) {
				trackInfo[fileName][segmentID]['nps_mission_code'] = selectedMission; //capture value
			}
		}
		// Disable it and set the value as null
		$('#select-nps_mission_code')
			.addClass('select-disabled')
			.attr('disabled', 'true')
			.val('');
	// If it is NPS, make sure mission_code select is enabled
	} else {
		$('#label-nps_mission_code').removeClass('select-disabled-label');
		$('#select-nps_mission_code')
			.removeClass('select-disabled')
			.removeAttr('disabled');
		// Set the value to whatever was recorded before disabling
		var thisInfo = trackInfo[fileName];
		if (thisInfo !== undefined) {
			for (segmentID in thisInfo) {
				$('#select-nps_mission_code').val(thisInfo[segmentID]['nps_mission_code'].length ? thisInfo[segmentID]['nps_mission_code'] : '');
				break // just assign with the first one
			}
			
		}
	}

}


function lockButtonClick() {

	var lockButton = $('#button-track-info-lock')
	var infoForm = $('#form-track-info')
	if (lockButton.hasClass('unlocked')) {
		lockButton.removeClass('unlocked')
		infoForm.find('input[type="text"]')
			.addClass('locked')
			.attr('disabled', 'true');
		infoForm.find('select')
			.addClass('locked')
			.attr('disabled', 'true');
	} else {
		lockButton.addClass('unlocked');
		infoForm.find('input[type="text"]')
			.removeClass('locked')
			.removeAttr('disabled');
		infoForm.find('select').removeClass('locked')
			.removeClass('locked')
			.removeAttr('disabled');
	}

	// Check to see if mission code should be .disabled
	onOperatorChange($('#select-operator_code').val());

	var fileName = getSelectedFileName();
	if (trackInfo[fileName] !== undefined) {
		for (segmentID in trackInfo[fileName]) {
			trackInfo[fileName][segmentID]['trackInfoUnlocked'] = lockButton.hasClass('unlocked');
		}
	}
}


function addMapNavToolbar() {
	/* Add a toolbar for zooming to full extent and the extent of the selected track*/

	var zoomSelected = L.Toolbar2.Action.extend({
			options: {
					toolbarIcon: {
							html: '<img src="imgs/zoom_selected_icon.svg"/>',
							tooltip: 'Zoom to selected track'
					}
			},
			addHooks: function () {
				var fileName = getSelectedFileName();
				if (fileName !== undefined) {
					map.fitBounds(lineLayers[fileName][selectedLines[fileName]].getBounds());
				} else {
					alert('The map is currently loading. Please wait to zoom until all tracks are loaded.');
				}
			}
				
	});

	var zoomFull = L.Toolbar2.Action.extend({
			options: {
					toolbarIcon: {
							html: '<img src="imgs/zoom_full_icon.svg"/>',
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

	new L.Toolbar2.Control({
			actions: [zoomSelected, zoomFull],
			position: 'topleft'
	}).addTo(map);
}


function validateTrackInfo() {


    var trackInfoInputs = $('.track-info-textbox, .track-info-textbox.locked');
    for (elementID in trackInfoInputs) {
        var thisElement = $(trackInfoInputs[elementID]);
        var thisID = thisElement[0].id;
        if (!thisElement.val().length && !thisID.includes('submitter_notes') && !thisElement.hasClass('select-disabled')){
            var thisLabel = $(thisElement.siblings()[0]).text();
            alert(`The "${thisLabel}" field is empty but all track info fields are mandatory.`);
            thisElement.focus();
            return false;
        }
    }

    if (!$('#textbox-registration').val().match(/N\d{2,5}[A-Z]{0,2}/)) {
        alert(`The "Tail number" field entry isn't valid.`);
        $('#textbox-registration').focus();
        return false;
    }

    if ($('#select-operator_code').val() === 'National Park Service' && !$('#select-nps_mission_code').val().length){
        alert(`You must select an "NPS mission code" if the operator is the National Park Service`);
        $('#select-nps_mission_code').focus();
        return false;
    }

    return true;
}


function importData(){

    var fileName = getSelectedFileName();
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
            return;
        }
    }

    var thisGeojson = {
        type: "FeatureCollection",
        crs: { type: "name", properties: { name: "urn:ogc:def:crs:OGC:1.3:CRS84" } },
        features: features
    };

    var thisTrackInfo = {...trackInfo[fileName][Object.keys(trackInfo[fileName])[0]]};
    thisTrackInfo['track_editor'] = $('#textbox-track_editor').val();

    console.log(`python ..\\scripts\\import_from_editor.py '../web/${filePath}' '../web/${filePath.replace('.geojson', '_track_info.json')}'' \\\\inpdenards\\overflights\\config\\poll_feature_service_params.json`)
    // send a post request to the PHP script
    var trackInfoPath = filePath.replace('.geojson', '_track_info.json')
    var stderrPath = filePath.replace('data/', 'errorLogs').replace('.geojson', `_${Date.now()}.err`);
    var data = {
        action: "importData",
        geojsonString: filePath,//JSON.stringify(thisGeojson).replace('"', '\\"'),
        trackInfoString: trackInfoPath, //JSON.stringify(thisTrackInfo).replace('"', '\\"')
        stderrPath: stderrPath
    };
    return;

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
                        var lines = stderr.split('\n');
                        var errorName = stderr.match(/[A-Z][a-z]*Error/)
                        var error;
                        for (lineNumber in lines) {
                            if (lines[lineNumber].startsWith(errorName)) {
                                error = lines[lineNumber];
                                break;
                            }
                        }

                        alert(`An error occurred while trying to import the data: ${error}. If you can't resolve this 
                            issue yourself, please contact the overflight data steward at ${dataSteward}`);

                    } else {
                        alert(importResponse.replace(/\t/g, ''));
                        
                        // Remove the file from the menu and delete it
                        removeFile(fileName);
                    }
                    // delete error log
                    //$.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: stderrPath},cache:false});
                }
            }).fail(function () {                        
                // make sure the file error log is deleted even if reading it failed
                //$.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: stderrPath},cache:false});
            });

            // Try to delete data
            $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: filePath}, cache:false});
            $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: trackInfoPath}, cache:false});

        }
    })
    .fail(function() {
        // if importing data failed, make sure the temp data are deleted
        $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: filePath}, cache:false});
        $.ajax({url:'geojson_io.php', method:'POST', data:{action: 'deleteFile', filePath: trackInfoPath}, cache:false});
    })

}

function onImportDataClick(){

    if (!validateTrackInfo()) return;

	var fileName = getSelectedFileName();
	var filePath = `data/edited/${fileName}.geojson`;
	
	// The point layes are in the format {segID: {geojson}} so just combine all of 
	//  the features into a single FeatureCollection GeoJSON object since the 
	//  segment IDs are also recorded in the properties of each feature
	var features = [];
	for (segmentID in pointGeojsonLayers[fileName]) {
		var geojson = pointGeojsonLayers[fileName][segmentID].toGeoJSON();
        features = features.concat(geojson.features)
	}
	var thisGeojson = {
		type: "FeatureCollection",
		crs: { type: "name", properties: { name: "urn:ogc:def:crs:OGC:1.3:CRS84" } },
		features: features
	}

	// send a post request to the PHP script
	var data = {
		action: "writeFile",
		filePath: filePath,
		jsonString: JSON.stringify(thisGeojson)
	};

	$.ajax({
		url: 'geojson_io.php',
		method: 'POST',
		data: data,
        cache: false,
		success: function(response) {
			if (response == false) {
				alert('JSON data failed to save')
			}
		}
	})
	.done(function() {
        var thisTrackInfo = {...trackInfo[fileName][Object.keys(trackInfo[fileName])[0]]}
        thisTrackInfo['track_editor'] = $('#textbox-track_editor').val()//set here because it might not have been
		$.ajax({
			url: 'geojson_io.php',
			method: 'POST',
            cache: false,
			data: {
				action: 'writeFile',
				filePath: filePath.replace('.geojson', '_track_info.json'),
				jsonString: JSON.stringify(thisTrackInfo)
			},
			success: function(response) {
				if (response == false) {
					alert('JSON track info failed to save')
				} else {
                    //alert('JSON saved to: ' + filePath)
                }
			},
            error: function(xhr, status, error) {
                alert(`JSON track failed to save because of a ${status} error: ${error}`)
            }
		})
        .done(importData);
	})



}