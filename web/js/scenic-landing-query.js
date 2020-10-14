

//import * as flights from './flights.js';
//const dataSteward = 'dena_flight_data@nps.gov';
//import {Color, Solver} from './color-filter.js';

const flightColumns = [
	'ticket',
	'departure date',
	'departure time',
	'registration',
	'scenic route',
	'fee/passenger',
	'total fee',
	'flight notes'
];
const landingColumns = [
	"landing location", 
	"landing passengers", 
	"landing type", 
	"landing notes", 
	"justification"
];

const editors = [
	'shooper',
	'amaki'
];

var landingQueryResult = {}; //global var to store result for writing CSV  
var currentControlVal; // used in .change() events to decide whether function should be fired
var editedFlights = []; // global var to keep track of data that have been edited (stores flight IDs)
var landingTypeOptions = ''; // Store results of query
var landingLocationOptions = [];
var cloneableLanding;
var cloneableFlight;
var username = '';

/* Extentions */
Date.prototype.addDays = function(days) {
    var date = new Date(this.valueOf());
    date.setDate(date.getDate() + days);
    return date;
}


Date.prototype.getChromeFormattedString = function() {

	let month = '0' + (this.getMonth() + 1);
	let day = '0' + (this.getDate())
	return `${this.getFullYear()}-${month.slice(month.length - 2, month.length)}-${day.slice(day.length - 2, day.length)}`;
}


// jquery pseudo-selectors to determine if ellipsis is active or not
$.expr[':'].truncated = function(jqObject) {
	return (Math.ceil($(jqObject).outerWidth()) < $(jqObject)[0].scrollWidth);
}

$.expr[':'].extended = function(jqObject) {
	return (Math.ceil($(jqObject).outerWidth()) >= $(jqObject)[0].scrollWidth);
}

// JQuery plugin to move cursor to end of text/textare inputs on focus
//	from: https://css-tricks.com/snippets/jquery/move-cursor-to-end-of-textarea-or-input/
jQuery.fn.setCursorToEnd = function() {

  return this.each(function() {
    
    // Cache references
    var $el = $(this),
        el = this;

    // Only focus if input isn't already
    if (!$el.is(":focus")) {
     $el.focus();
    }

    // If this function exists... (IE 9+)
    if (el.setSelectionRange) {

      // Double the length because Opera is inconsistent about whether a carriage return is one character or two.
      var len = $el.val().length * 2;
      
      // Timeout seems to be required for Blink
      setTimeout(function() {
        el.setSelectionRange(len, len);
      }, 1);
    
    } else {
      
      // As a fallback, replace the contents with itself
      // Doesn't work in Chrome, but Chrome supports setSelectionRange
      $el.val($el.val());
      
    }

    // Scroll to the bottom, in case we're in a tall textarea
    // (Necessary for Firefox and Chrome)
    this.scrollTop = 999999;

  });

};


function fillSelectOptions(selectElementID, queryString, dbname, optionClassName='track-info-option') {
    

    var deferred = $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'query', dbname: dbname, queryString: queryString},
        cache: false,
        success: function(queryResultString){
            var queryResult = queryResultString.startsWith('ERROR') || queryResultString === '["query returned an empty result"]' ? 
            	false : $.parseJSON(queryResultString);
            if (queryResult) {
                queryResult.forEach(function(object) {
                    $('#' + selectElementID).append(
                        `<option class="${optionClassName}" value="${object.value}">${object.name}</option>`
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


function requerySecondarySelects() {//').change(function(){//, #input-start_date, #input-end_date').change(function() {//
	
	const startDate = $('#input-start_date').val();
	const endDate = $('#input-end_date').val();
	const tickets = $('#select-tickets').val().join(',');
	const ticketSearchClause = tickets.length ? ` AND flights.ticket IN (${tickets})` : '';
	const locations = $('#select-locations').val().join(`', '`);
	const locationSearchClause = locations.length ? ` AND landings.location IN ('${locations}')` : '';
	
	$('#select-tickets').empty();
	$('#select-locations').empty();
	
	const ticketSQL = `
		SELECT DISTINCT ticket as name, ticket AS value 
		FROM flights INNER JOIN landings ON flights.id = landings.flight_id 
		WHERE 
			departure_datetime::date BETWEEN '${startDate}' AND '${endDate}' 
			AND operator_code='${$('#select-operator').val()}'
			${locationSearchClause}
		ORDER BY ticket DESC;
	`;
	const locationSQL = `
		SELECT DISTINCT landing_locations.name AS name, landing_locations.code AS value 
		FROM landing_locations 
			INNER JOIN landings ON landing_locations.code = landings.location 
			INNER JOIN flights ON flights.id = landings.flight_id 
		WHERE 
			departure_datetime::date BETWEEN '${startDate}' AND '${endDate}' 
			AND operator_code='${$('#select-operator').val()}' 
			${ticketSearchClause}
		ORDER BY name DESC;
	`;
	showLoadingIndicator();
	$.when(
		fillSelectOptions('select-tickets', ticketSQL, 'scenic_landings'),
		fillSelectOptions('select-locations', locationSQL, 'scenic_landings')
	).then(hideLoadingIndicator());
}


function getSanitizedFieldName(fieldName) {
	/*Helper function to remove special characters from query result field names*/
	return fieldName.replace(/[\W]+/g, '_')
}


function queryFlights() {
	const searchByTicket = $(`#search-by-slider-container input[type='checkbox']`).get(0).checked
	const ticketNumbers = searchByTicket ? $('#select-tickets-only').val().join(',') : $('#select-tickets').val().join(',');
	const operator_code = $('#select-operator').val();
	if (searchByTicket) {
		if (!ticketNumbers.length) {
			alert('You must enter at least one ticket number before running a query');
			return;
		}
	} else {
		if (!operator_code.length) {
			alert('You must select on operator first before running a query');
			return;
		}
	}

	
	const ticketSearchClause = (ticketNumbers.length) ? ` AND ticket IN (${ticketNumbers})` : '';

	const locations = $('#select-locations').val().join(`', '`);
	const locationSearchClause = (locations.length) ? ` AND landings.location IN ('${locations}')` : '';

	const start_date = $('#input-start_date').val();
	const end_date = $('#input-end_date').val();
	let conditions; 
	if (searchByTicket) {
		conditions = ` ticket IN (${ticketNumbers}) `
	} else {
		conditions = `
			flights.departure_datetime::date BETWEEN '${start_date}' AND '${end_date}' AND 
			flights.operator_code = '${operator_code}'
			${ticketSearchClause}
			${locationSearchClause}
		`;
	}

	const sql = `
		SELECT 
			flights.id, 
			flights.ticket,
			flights.registration,
			flights.flight_id,
			flights.operator_code,
			flights.fee_per_passenger AS "fee/passenger",
			flights.departure_datetime::date AS "departure date", 
			to_char(flights.departure_datetime, 'HH24:MI:SS') AS "departure time", 
			coalesce(flights.scenic_route, '') AS "scenic route",
			coalesce(flights.notes, '') AS "flight notes", 
			coalesce(fees.n_fee_passengers, 0) AS "fee_passengers", 
			coalesce(fees.fee, 0) AS "total fee", 
			landings.location AS "landing location",
			landings.id AS landing_id,  
			landings.n_passengers AS "landing passengers", 
			landings.landing_type AS "landing type", 
			coalesce(landings.notes, '') AS "landing notes", 
			coalesce(landings.justification, '') AS justification  
		FROM 
			flights 
		LEFT JOIN 
			concession_fees_all_flights_view fees ON flights.id = fees.flight_id 
		LEFT JOIN 
			landings ON flights.id = landings.flight_id 
		LEFT JOIN 
			landing_locations ON landings.location = landing_locations.code 
		WHERE ${conditions}
		ORDER BY
			flights.departure_datetime,
			landings.id
		; 
	`;

	showLoadingIndicator();
	var deferred = $.ajax({
		url: 'geojson_io.php',
		method: 'POST',
		data: {action: 'query', dbname: 'scenic_landings', queryString: sql},
		cache: false,
		success: function(queryResultString){
			var queryResult = queryResultString.trim().startsWith('ERROR') ? false : $.parseJSON(queryResultString);
			if (queryResult)  {
				if (queryResult[0] === 'query returned an empty result') {
					alert(`There were no scenic landings for ${$('#select-operator option:selected').text()} during the selected date range`);
					hideLoadingIndicator();
				} else {
					//landingQueryResult['operator'] = $('#select-operator option:selected').text();
					//landingQueryResult = {...queryResult};//landingQueryResult['data'] = [...queryResult];//
					landingQueryResult.fields = {
						'flights': {},
						'landings': {}
					};
					landingQueryResult.data = {};
					for (i in queryResult) {
						let row = queryResult[i];
						let thisFlightID = row.id;
						if (landingQueryResult.data[thisFlightID] === undefined) {
							landingQueryResult.data[thisFlightID] = {};
							for (fieldName in row) {
								
								//let fieldName = flightColumns[i];
								let columnID = getSanitizedFieldName(fieldName);
								landingQueryResult.data[thisFlightID][columnID] = row[fieldName];
								if (flightColumns.includes(fieldName))  {
									landingQueryResult.fields.flights[columnID] = fieldName;
								}
								
							}
							landingQueryResult.data[thisFlightID].landings = {};
							landingQueryResult.data[thisFlightID].landingsOrder = [];
						}

						let thisLanding = {};
						for (fieldName in row) {
							//let fieldName = landingColumns[i];
							let columnID = getSanitizedFieldName(fieldName);
							thisLanding[columnID] = row[fieldName];
							if (landingColumns.includes(fieldName)) {
								landingQueryResult.fields.landings[columnID] = fieldName;
							}
						}
						landingQueryResult.data[thisFlightID].landings[row.landing_id] = thisLanding;
						landingQueryResult.data[thisFlightID].landingsOrder.push(row.landing_id);
					}


					/*landingQueryResult.data = {};
					landingQueryResult.fields ={};
					for (i in queryResult) {
						let thisFlight = queryResult[i];
						landingQueryResult.data[thisFlight.id] = {};
						for (field in thisFlight) {
							let cleanedFieldName = getSanitizedFieldName(field);
							if (landingQueryResult.fields[cleanedFieldName] === undefined) {
								landingQueryResult.fields[cleanedFieldName] = field;
							}
							landingQueryResult.data[thisFlight.id][cleanedFieldName] = thisFlight[field];
						}
					}*/

					showQueryResult().then(() => {
						hideLoadingIndicator();//'queryFlights');
						// Show the first flight's landings after a brief delay
						setTimeout(() => {$('.card-header > a').get(0).click()}, 300);
					})
				}
			} else {
				console.log(`error running query: ${queryResultString}`);
			}
		}
	}).fail((xhr, status, error) => {
		console.log(`query failed with status ${status} because ${error} from query:\n${sql}`)
	});
}


function onRunClick(event) {

	// prevent the form from resetting
	event.returnValue = false;

	// If there are unsaved edits, ask if the user wants to save them
	let deferred = $.when(function() {return true});
	if ($('.flight-data-dirty').length) {
		if (confirm(`You have unsaved edits. Click 'OK' to save them or 'Cancel' to discard them`)) {
			deferred = saveEdits($('.flight-data-dirty').attr('id').replace('flight-card-', ''))
		} 
	}

	deferred.then(queryFlights);

}


function getTextWidth(textString, font) { 

    canvas = document.createElement('canvas'); 
    context = canvas.getContext('2d'); 
    context.font = font; 
    width = Math.ceil(context.measureText(textString).width); 

    return width;
}


function removeModalExpandedText() {
	
	$('.modal-background').remove();
	$('.modal-content').remove();
}


function showExpandedText(cellID) {

	// Prevent the 
	//event.stopPropagation();

	let thisCell = $('#' + cellID);
	let truncatable = thisCell.find('.truncatable');
	let text = truncatable.val();//.text();
	let font = truncatable.css('font');
	let textWidth = getTextWidth(text, font);
	let nLines = Math.ceil(textWidth / 400) + 2;
	let position = truncatable.position();
	
	// If the truncatable div doesn't have it's own background color, find the first parent that does
	let bgColorChannels = truncatable.parent().css('background-color').replace('rgba(','').replace(')', '').split(', ')
	let alpha = bgColorChannels[3] // will just return undefined if 4 channels not found
	var thisBackgroundColor;
	if (alpha == 0) {
		let opaqueBgParents = truncatable.parents().filter(function() {
			let bgColorChannels = $(this).css('background-color').replace('rgba(','').replace(')', '').split(', ')
		    return bgColorChannels[3] != 0}
		)
		thisBackgroundColor = opaqueBgParents.length ? $(opaqueBgParents.get(0)).css('background-color') : '#894C4C'
	} else {
 		thisBackgroundColor = truncatable.parent().css('background-color')
	}

	// Add the modal div
	let modalHeight = truncatable.css('line-height').replace('px', '') * nLines;
	let foreColor = truncatable.css('color');
	let truncatableStyle = 
		`
		background-color: ${thisBackgroundColor}; 
		color: ${foreColor};
		height: ${modalHeight + 65}px;
		`;
	const foreColorRGB = foreColor.replace('rgb(', '').replace(')', '').split(', ');
	const color = new Color(foreColorRGB[0], foreColorRGB[1], foreColorRGB[2]);
	const solver = new Solver(color);
	const result = solver.solve();
	const filterString = result.filter;
	
	$(`
		<div class="modal-background"></div>
		<div class="modal-content" style="${truncatableStyle}" data-parent="${truncatable.attr('id')}">
			<div class="modal-button-container">
				<span class="save-modal-text-button-container">
					<img class="save-modal-text-button slide-up-on-hover" src="imgs/save_icon_30px.svg" data-text-source="${truncatable.attr('id')}" style="${filterString}">
				</span>
				<span class="close-modal-text-button slide-up-on-hover" style="color: ${foreColor}">&times;</span>
			</div>
			<div class="row mx-0 px-0" style="display: flex; justify-content: center; width: 100%; height: ${modalHeight}px;">
				<textarea class="modal-expanded-text" style="width: 100%; height: ${modalHeight}px;">${text}</textarea>
			</div>
		</div>
	`).appendTo('body');

	$('.modal-background, .close-modal-text-button').click(removeModalExpandedText);
	$('.save-modal-text-button').click(function() {
		let sourceID = $(this).attr('data-text-source');
		let newText = $(this).closest('.modal-content').find('.modal-expanded-text').val();
		$('#' + sourceID).val(newText);
		removeModalExpandedText();
		removeExpandTruncatedButtons();
	})

	$('.modal-expanded-text')
		.focus( function() {$(this).setCursorToEnd()}) // set the listener
		.focus(); // trigger the event

}	


function addExpandTruncatedButton(parentCell, thisElement) {
	if (!parentCell.find('.expand-truncated-button').length) { 
		let thisTextColor = thisElement.css('color');
		$(`
			<button class="expand-truncated-button slide-up-on-hover">
				<h4 style="color: ${thisTextColor};">+</h4>
			</button>
		`).click((event) => {
			event.stopPropagation();
			showExpandedText(parentCell.attr('id'));
		})
		.appendTo(parentCell);
		thisElement.addClass('truncated');
	}
}


function removeExpandTruncatedButtons() {
	// find any inputs that were truncated, but are now fully visible and remove the expand button
	
	$('.result-table-cell > .truncatable:extended, .landing-table-cell > .truncatable:extended').each(function(){
		let thisCell = $(this).parent();
		thisCell.find('.expand-truncated-button').remove();
		thisCell.find('.truncated').removeClass('truncated');
	})
}


function resizeColumns() {

	if (landingQueryResult.data === undefined) return;

	for (columnID in landingQueryResult.fields.flights) {
		//let columnID = getSanitizedFieldName(flightColumns[i]) //flightColumns[i].replace(/[\W]+/g, '_');
		try {
			//let padding = parseInt($(`#column-${columnID}`).css('padding-left').replace('px', '')) * 2;
			let columnWidth = $(`#column-${columnID}`).outerWidth() //+ padding;
			$('#flight-result-container').find(`.result-table-cell.cell-${columnID}`).css('width', columnWidth);
		} catch {
			continue;
		}
	}

	// find any divs truncated because their too long and add a button to show the full text
	$('.result-table-cell > .truncatable:truncated').each(function(){
		addExpandTruncatedButton($(this).parent(), $(this));
	});

	removeExpandTruncatedButtons();
}


function stringToNumber(str) {
	
	if (isNaN(str)) {
		return str;
	} else if (!isNaN(str) && str.toString().indexOf('.') != -1) {
		return parseFloat(str);
	} else {
		return parseInt(str);
	}
}


function insertLanding(landingRow, flightID, landingColumns, commit=true) {

	let sqlColumns = [];
	let sqlValues = [];
	let sqlParameters = [];
	let paramCount = 1;
	let landingObject = {}
	landingRow.find('.landing-result-input').each(
		function() {
			let columnID = $(this).attr('data-column-id');
			let newVal = $(this).val();

			landingObject[columnID] = newVal;
			
			// Add set clause to update DB
			sqlColumns.push(landingColumns[columnID]);
			sqlValues.push(newVal);
			sqlParameters.push(`$${paramCount}`);
			
			paramCount ++;
		})

	sqlColumns.push('flight_id');
	sqlValues.push(flightID);
	sqlParameters.push(`$${paramCount}`);
	
	let sqlString = `INSERT INTO landings (${sqlColumns.join(', ')}) VALUES(${sqlParameters.join(', ')}) RETURNING id;`;	

	if (commit) {
		return $.ajax({
	        url: 'geojson_io.php',
	        method: 'POST',
	        data: {action: 'landingsParamQuery', dbname: 'scenic_landings', queryString: sqlString, params: sqlValues},
	        cache: false,
	        success: function(queryResultString){
	        	let resultString = queryResultString.trim();
	        	if (resultString.startsWith('ERROR') || resultString === "false") {
	        		alert('Unable to add a new landing to the database. ' + resultString);
	        		return false;
	        	} else {
	        		let result = $.parseJSON(resultString);
	        		if (result.length) {
	        			let landingID = result[0].id;
	 					// Add to the in-memory query result object
	        			landingQueryResult.data[flightID].landings[landingID] = {...landingObject};
	        			// Remove temp class and set the landingID for the <tr> element
	        		    landingRow.removeClass('provisional')
	        				.attr('data-landing-id', landingID)
	        		}
	        		return true;
	        	}
	        }
	    })
	} else {
		return true;// no attemp to save to db to just return true
	}
}


function deleteLanding(landingRow, flightID) {

	let landingID = landingRow.attr('data-landing-id')
	let sqlString = `DELETE FROM landings WHERE id=${landingID};`;
	$.ajax({
		url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'landingsAdminQuery', dbname: 'scenic_landings', queryString: sqlString},
        cache: false,
        success: function(queryResultString){
        	let resultString = queryResultString.trim();
        	if (resultString.startsWith('ERROR') || resultString === "false") {
        		alert(`Unable to delete landing ${landingID} from the database. ${resultString}`);
        		// Show the user which landing failed
        		landingRow.show(300).removeClass('to-delete');
        	} else {
        		delete landingQueryResult.data[flightID].landings[landingID];
        		landingRow.remove(); //permanently remove from DOM
        	}
        }
	})
}


function isFlightIDStringDuplicated(flightIDString, flightID=null) {

	let idClause = flightID === null ? '' : `AND id<>${flightID}`;
	
	return $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'query', dbname: 'scenic_landings', queryString: `SELECT * FROM flights WHERE flight_id='${flightIDString} ${idClause}';`},
        cache: false
    }).then(
    	function(queryResultString){
    		queryResultString = queryResultString.trim();
            if (queryResultString !== '["query returned an empty result"]') {
            	alert('The data for this flight could not be saved because there ' + 
            		'is already a flight with this departure date/time and registraton number. ' + 
            		'Please check to make sure you have entered these correctly.')
            	return true;
            } else if (queryResultString.startsWith('ERROR')) {
            	alert('An error occurred while validating the flight data: ' + queryResultString)
            	return true;
            } else {
            	return false;
            }
    	}
    );
}


function getFlightSQLParams(dbColumns, flightID, departureDatetime, flightIDString, statementType='update') {
	// collect values from inputs and landings table
	
	let thisCardHeader = $(`#cardHeader-${flightID}`);
	
	var parametizedFields = [];
	let sqlValues = []; //gets overwritten with each landing statement
	let paramCount = 1;
	thisCardHeader
		.find('.flight-result-input')
		.filter(function(){
			return !(
				$(this).hasClass('flight-result-input-disabled') ||
				$(this).parent().hasClass('cell-departure_date') ||
				$(this).parent().hasClass('cell-departure_time') ||
				$(this).parent().hasClass('cell-ticket')
				);
		})
		.each(function() {
			let columnID = $(this).attr('data-column-id');
			let newVal = $(this).val();
			
			// Update in-memory query result
			landingQueryResult.data[flightID][columnID] = newVal;
			
			// Add set clause to update DB
			let cleanedNewVal = newVal.replace('$', '')
			let thisSQLVal = cleanedNewVal;//newVal.length ? cleanedNewVal : undefined;//undefined -> php null -> SQL NULL
			let thisDBColumn = dbColumns.flights[columnID];
			parametizedFields.push(`${thisDBColumn}=$${paramCount}`);
			sqlValues.push(thisSQLVal);
			paramCount ++;
		});
	
	// Add departure_datetime and update flight ID
	sqlValues.push(departureDatetime);
	parametizedFields.push(`departure_datetime=$${paramCount}`);
	sqlValues.push(flightIDString);
	parametizedFields.push(`flight_id=$${paramCount + 1}`);

	return [sqlValues, parametizedFields];
}


function updateCardIDs(cardElement, flightID) {
	cardElement.removeClass('provisional')
		.attr('id', `flight-card-${flightID}`)
	cardElement.find('.card-header')
		.attr('id', `cardHeader-${flightID}`)
	cardElement.find('.card-header > a')
		.attr('href', `#cardContent-${flightID}`)
	cardElement.find('.collapse')
		.attr('id', `cardContent-${flightID}`)
		.attr('aria-labeledby', `cardHeader-${flightID}`)
	cardElement.find('.delete-flight-button')
		.attr('id', `delete-${flightID}`)
	cardElement.find('.edit-button')
		.attr('id', `edit-${flightID}`)
}


function insertFlight(dbColumns, flightID, departureDatetime, flightIDString) {
	/*
	In sequence:
		1. If no ticket was given, add one
		2. With the ticket number, insert a flight and get the flight's id
		3. With the id as flight_id in the landing table, insert the landing
			- if this fails, delete the flight
	*/

	showLoadingIndicator();

	let thisCard = $('.card.provisional') // should only be one with .provisional class

	let ticket = thisCard.find('.cell-ticket > .flight-result-input').val();
	let deferred = $.when(true);
	if (isNaN(ticket)) {
		let dt = new Date();
		let datetimeString = `${dt.getFullYear()}-${dt.getMonth() + 1}-${dt.getDate()} ${dt.getHours()}:${dt.getMinutes()}`
		let submissionSQL = `INSERT INTO submissions (submitter, submission_time) VALUES ('${username}', '${datetimeString}') RETURNING ticket;`;
		deferred = 
			$.ajax({
		 		url: 'geojson_io.php',
		 		method: 'POST',
		 		data: {action: 'landingsAdminQuery', dbname: 'scenic_landings', queryString: submissionSQL},
		 		cache: false
		    }).then( 
		    	function(queryResultString){
		        	let resultString = queryResultString.trim();
		        	if (resultString.startsWith('ERROR') || resultString === "false") {
		        		alert('Unable to add a new landing to the database. ' + resultString);
		        		return false;
		        	} else {
		        		let result = $.parseJSON(resultString);
		        		if (result.length) {
		        			let ticket = result[0].ticket;
		        			return true; // if there wasn't an error
		        		}
		        	}
		        }
		    )
	}

	let newFlightData = {};

	return deferred.then(function(ticketExists) {
		if (ticketExists) {
			var sqlStatements = []; // array of SQL statement strings for parametizing
			let sqlParameters = []; // array of arrays containing parameters for corresponding sqlStatements
			
			var fields = [];
			let sqlValues = []; //gets overwritten with each landing statement
			let paramCount = 1;
			var flightObject = {};
			thisCard
				.find('.flight-result-input')
				.filter(function(){
					return !(
						$(this).hasClass('flight-result-input-disabled') /*||
						$(this).parent().hasClass('cell-departure_date') ||
						$(this).parent().hasClass('cell-departure_time') ||
						$(this).parent().hasClass('cell-ticket')*/
						);
				})
				.each(function() {
					let columnID = $(this).attr('data-column-id');
					let newVal = $(this).val();
					flightObject[columnID] = newVal;

					if ($(this).parent().hasClass('cell-departure_date') ||
						$(this).parent().hasClass('cell-departure_time') ||
						$(this).parent().hasClass('cell-ticket')) {
						return; // Don't add these to the sql statement
					}

					// Add set clause to update DB
					let thisSQLVal = newVal.replace('$', '')
					newFlightData[columnID] = thisSQLVal;
					let thisDBColumn = dbColumns.flights[columnID];
					fields.push(thisDBColumn);
					sqlValues.push(thisSQLVal);
					paramCount ++;
				});
			
			// Add departure_datetime other fields
			sqlValues.push(departureDatetime);
			fields.push('departure_datetime');

			sqlValues.push(flightIDString);
			fields.push('flight_id');
			flightObject['flight_id'] = flightIDString;
			
			sqlValues.push(username);
			fields.push('edited_by');
			
			const now = new Date();
			sqlValues.push(`${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()} ${now.getHours()}:${now.getMinutes()}`);
			fields.push('last_edit_time');
			
			const operator = landingQueryResult.data[Object.keys(landingQueryResult.data)[0]].operator_code;
			sqlValues.push(operator);
			fields.push('operator_code');
			
			// Add ticket since it's normally disabled
			sqlValues.push(ticket);
			fields.push('ticket');
			
			let parametizedValues = [];
			for (i = 1; i <= fields.length; i++) {
				parametizedValues.push(`$${i}`);
			}

			let sql = `INSERT INTO flights (${fields.join(', ')}) VALUES(${parametizedValues.join(', ')}) RETURNING id;`;
			return $.when(
				$.ajax({
			        url: 'geojson_io.php',
			        method: 'POST',
			        data: {action: 'landingsParamQuery', dbname: 'scenic_landings', queryString: sql, params: sqlValues},
			        cache: false
		    	})
		    ).then(
		    	function(queryResultString){
		        	let resultString = queryResultString.trim();
		        	if (resultString.startsWith('ERROR') || resultString === "false") {
		        		alert('Unabled to save changes to the database. ' + resultString);
		        		return false; // Save was unsuccessful
		        		hideLoadingIndicator();
		        	} else {
		        		let result = $.parseJSON(resultString);
		        		if (result.length) {
		        			let flightID = result[0].id;
		 
		 					// Add to the in-memory query result object
		 					flightObject.landings = {};
		        			landingQueryResult.data[flightID] = {...flightObject};
		        			
		        			return $.when(insertLanding(thisCard.find('.landing-table-row'), flightID, dbColumns.landings))
		        			.then(function(success) {
		        				if (success) {
			        				// Remove temp class and set the flightID for the .card element
									updateCardIDs(thisCard, flightID)
			        				return true;
		        				} else {
		        					deleteFlightFromDB(flightID)//remove it
		        					return false;
		        				}
		        				hideLoadingIndicator();
		        			})
		        		} else {
		        			return false;//somehow the query still didn't work
		        		}
		        	}
		        	hideLoadingIndicator();
		        }
		    )
		}
	})

}


function updateFlight(dbColumns, flightID, departureDatetime, flightIDString) {

    let thisCardHeader = $(`#cardHeader-${flightID}`);

    // Premanently remove any provisional landings that the user deleted
    $('.provisional.to-delete').remove();

    // Check if there's a new landing that has yet to be saved. If so, insert it
    let unsavedLanding = $(`#flight-card-${flightID}`).find('.landing-table-row.provisional')
    if (unsavedLanding.length) {
		insertLanding(unsavedLanding, flightID, dbColumns.landings);
    }

    $(`#flight-card-${flightID}`).find('.landing-table-row.to-delete').each(function() {
    	deleteLanding($(this), flightID);
    });

	var sqlStatements = []; // array of SQL statement strings for parametizing
	let sqlParameters = []; // array of arrays containing parameters for corresponding sqlStatements
	
	// Get flight vals and parametized clauses (i.e. <field>=$1)
	/*let flightSQLValues, flightParametizedFields;
	[flightSQLValues, flightParametizedFields] = getFlightSQLParams(dbColumns, flightID, departureDatetime, flightIDString);*/
	var flightParametizedFields = [];
	let flightSQLValues = []; //gets overwritten with each landing statement
	let paramCount = 1;
	thisCardHeader
		.find('.flight-result-input')
		.filter(function(){
			return !(
				$(this).hasClass('flight-result-input-disabled') ||
				$(this).parent().hasClass('cell-departure_date') ||
				$(this).parent().hasClass('cell-departure_time') ||
				$(this).parent().hasClass('cell-ticket')
				);
		})
		.each(function() {
			let columnID = $(this).attr('data-column-id');
			let newVal = $(this).val();
			
			// Update in-memory query result
			landingQueryResult.data[flightID][columnID] = newVal;
			
			// Add set clause to update DB
			let cleanedNewVal = newVal.replace('$', '')
			let thisSQLVal = cleanedNewVal;//newVal.length ? cleanedNewVal : undefined;//undefined -> php null -> SQL NULL
			let thisDBColumn = dbColumns.flights[columnID];
			flightParametizedFields.push(`${thisDBColumn}=$${paramCount}`);
			flightSQLValues.push(thisSQLVal);
			paramCount ++;
		});
	
	// Add departure_datetime and update flight ID
	flightSQLValues.push(departureDatetime);
	flightParametizedFields.push(`departure_datetime=$${paramCount}`);
	paramCount ++;
	flightSQLValues.push(flightIDString);
	flightParametizedFields.push(`flight_id=$${paramCount}`);
	paramCount ++;
	flightSQLValues.push(username);
	flightParametizedFields.push(`edited_by=$${paramCount}`);
	paramCount ++;
	const now = new Date();
	flightSQLValues.push(`${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()} ${now.getHours()}:${now.getMinutes()}`);
	flightParametizedFields.push(`last_edit_time=$${paramCount}`);
	
	sqlParameters.push(flightSQLValues);
	sqlStatements.push(`UPDATE flights SET ${flightParametizedFields.join(', ')} WHERE id=${flightID};`);
	
	let sqlValues = []; //gets overwritten with each landing statement
	thisCardHeader.siblings() // the landing table is in the collapse
		.find('.landing-table-row')
		.each(function() {
			// If this is an unsaved landing or one to delete, it's getting inserted/deleted already, so skip it
			if ($(this).hasClass('provisional') || $(this).hasClass('to-delete')) {
				return;
			}

			let thisLandingID = $(this).attr('data-landing-id');
			// Loop through each <td> element
			let landingUpdates = [];
			let sqlValues = [];
			let paramCount = 1;
			$(this).find('.landing-result-input').each(function() {
				let thisColumnID = $(this).attr('data-column-id');
				let newVal = $(this).val();

				// Update in-memory query result object
				landingQueryResult.data[flightID].landings[thisLandingID][thisColumnID] = newVal;
				
				// Add set clause to update DB
				let thisDBColumn = dbColumns.landings[thisColumnID];
				let thisSQLVal = newVal//.length ? newVal : undefined;
				landingUpdates.push(`${thisDBColumn}=$${paramCount}`);
				sqlValues.push(thisSQLVal);
				paramCount ++;
			})
			let thisSQL = `UPDATE landings SET ${landingUpdates.join(', ')} WHERE id=${thisLandingID};`
			sqlStatements.push(thisSQL);
			sqlParameters.push(sqlValues);
		})

	
	// Update DB
	return $.when(
		$.ajax({
	        url: 'geojson_io.php',
	        method: 'POST',
	        data: {action: 'landingsParamQuery', dbname: 'scenic_landings', queryString: sqlStatements, params: sqlParameters},
	        cache: false
    	})
    ).then(
    	function(queryResultString){
        	let resultString = queryResultString.trim();
        	if (resultString.startsWith('ERROR') || resultString === "false") {
        		alert('Unabled to save changes to the database. ' + resultString);
        		return false; // Save was unsuccessful
        	} else {
        		return true; // Save was successful
        	}
        	hideLoadingIndicator();
        }
    )

}


function saveEdits(flightID) {

    const _dbColumns = {
    	'flights': {
			'scenic_route':  'scenic_route',
			'flight_notes':  'notes',
			'fee_passenger': 'fee_per_passenger',
			'registration':  'registration'
    	},
    	'landings': {
    		'landing_location': 	'location', 
			'landing_passengers': 	'n_passengers', 
			'landing_type': 		'landing_type', 
			'landing_notes': 		'notes', 
			'justification': 		'justification'
    	}
    }

    let thisCard = $(`#flight-card-${flightID}`)
    let thisCardHeader = $(`#cardHeader-${flightID}`);
    let dataSaved = false;

    showLoadingIndicator();

	// Check to make sure this doesn't violate the UNIQUE constraint on flight ID
	let departureDatetime = 
		thisCardHeader.find('.cell-departure_date > .flight-result-input').val() + 
		" " + 
		thisCardHeader.find('.cell-departure_time > .flight-result-input').val();
	let dt = new Date(departureDatetime);
	let registration = thisCardHeader.find('.cell-registration > .flight-result-input').val();
	let flightIDString = 
		registration + 
		'_' + 
		dt.getUTCFullYear() +
		("0" + (dt.getMonth()+1)).slice(-2) +
	    ("0" + dt.getDate()).slice(-2) +
	    ("0" + dt.getHours()).slice(-2) +
	    ("0" + dt.getMinutes()).slice(-2);

	// If this is a newly added flight, first check if the departure datetime/registration 
	//	combination already exist. If it does, don't INSERT intot the db
	if (thisCard.hasClass('provisional')) {
		return $.when(
			isFlightIDStringDuplicated(flightIDString)
		).then(function(duplicated) {
			if (!duplicated) {
				return insertFlight(_dbColumns, flightID, departureDatetime, flightIDString);
			} else {
				return false;
			}
		});
	// If the flight already exists, try to update the existing DB records
	} else {
		// If the departure datetime and/or the registration have changed, first check to see if 
		//	this flight already exists according to those values. Update the flight only if it 
		//	doesn't already exist
		if (flightIDString !== landingQueryResult.data[flightID].flight_id) {
			return $.when(
				isFlightIDStringDuplicated(flightIDString, flightID)
			).then(function(duplicated) {
				if (!duplicated) {
					return updateFlight(_dbColumns, flightID, departureDatetime, flightIDString);
				} else {
					return false; // data were not saved
				}
			});
		// Otherwise, just update the flight
		} else {
			return updateFlight(_dbColumns, flightID, departureDatetime, flightIDString);
		}
	}

}


function discardEdits(cardElement) {

	// If this card is a new one that hasn't been saved, just remove it
	if (cardElement.hasClass('provisional')) {
		cardElement.remove();
		return;
	}

	// Remove unsaved landing row if there is one
	cardElement.find('.provisional').remove();

	let flightID = cardElement.attr('id').replace('flight-card-', '');
	cardElement.find('.flight-result-input, .landing-result-input').each(function(){
		if ($(this).hasClass('flight-result-input-disabled') || $(this).hasClass('landing-result-input-disabled')) {
			return;
		}
		let columnID = $(this).attr('data-column-id');

		let thisValue;
		if (Object.keys(landingQueryResult.fields.landings).includes(columnID)) {
			const landingID = $(this).closest('tr').attr('data-landing-id');
			thisValue = landingQueryResult.data[flightID].landings[landingID][columnID]
		} else {
			thisValue = landingQueryResult.data[flightID][columnID];
		}
		$(this).val(columnID === 'fee_passenger' && !thisValue.startsWith('$') ? '$' + thisValue : thisValue);
	})

	removeExpandTruncatedButtons();

	// Remove the "to-delete" class indicating that those rows should be deleted
	$('.to-delete').show(400, function() {
		$(this).removeClass('to-delete')
	})

	// Call the change event on each input to see if anything that's not directly editabled needs to be reverted back
	$('.flight-result-input, .landing-result-input').each(function() {
		if ($(this).hasClass('flight-result-input-disabled') || $(this).hasClass('landing-result-input-disabled')) return;
		$(this).change();
	})

	cardElement.removeClass('flight-data-dirty');
	$('#save-edits-button').addClass('hidden');

	return $.when(true);

}


async function confirmCurrentEdits() {
	
	let enabledFlightInputs = $('#result-table').find('.flight-result-input').filter(function(){
		return !$(this).hasClass('flight-result-input-disabled')
	})
	let enabledLandingInputs = $('#result-table').find('.landing-result-input').filter(function(){
		return !$(this).hasClass('landing-result-input-disabled')
	})
	if (enabledFlightInputs.length) {
		// If there are unsaved edits, ask the user if they want to save them
		let previousCard = enabledFlightInputs.closest('.card')
		let deferred;
		if (previousCard.hasClass('flight-data-dirty')) {
			let response = confirm(`You are currently editing another flight. Click 'OK' to save these edits or 'Cancel' to discard them.`);
			if (response) {
				deferred = saveEdits(previousCard.attr('id').replace('flight-card-', ''));
			} else {
				deferred = discardEdits(previousCard);
			}
			previousCard.removeClass('flight-data-dirty');
		} else {
			deferred = $.when(function() {return true});
		}
		deferred.then(function(success) {
			if (success) {
				enabledFlightInputs.addClass('flight-result-input-disabled');
				enabledLandingInputs.addClass('landing-result-input-disabled');
				$('.edit-button').removeClass('white-haloed'); //remove halo from all (the only) active edit button
				$('.delete-flight-button').addClass('hidden');
			}
		});
		return deferred;
	} else {
		// return a deferred that resolves to true so other calling functions 
		//	know they can assume any data were sucessfully saved
		return $.when(function() {return true});
	}
}


function unfocusEditButton(saveSuccessful, thisCard, thisCardHeader) {
	
	if (saveSuccessful) {
		thisCard.removeClass('flight-data-dirty');
		$('#save-edits-button').addClass('hidden');
		thisCardHeader.find('.flight-result-input').addClass('flight-result-input-disabled')
		thisCardHeader.siblings().find('.landing-result-input').addClass('landing-result-input-disabled');
		thisCardHeader.siblings().find('.delete-landing-button, .add-landing-button').addClass('hidden');
		thisCardHeader.find('.delete-flight-button, .delete-landing-button').addClass('hidden');
		thisCardHeader.find('.edit-button').removeClass('white-haloed');
	}
}


function saveEditsAndDisable(cardElement) {
	deferred = saveEdits(cardElement.attr('id').replace('flight-card-', ''))							
	deferred.then((saveSucessful) => {
					unfocusEditButton(saveSucessful, cardElement, cardElement.find('.card-header'));
				});
	return deferred;
}


function onSaveEditsButtonClick() {
	let currentCard = $('.card.flight-data-dirty');
	saveEditsAndDisable(currentCard);
}


function onEditButtonClick() {
	let thisCard = $(this).closest('.card')
	let thisCardHeader = thisCard.find('.card-header');
	let deferred;
	// If there are any disabled inputs, they're all disabled and this row is not currently editable
	if (thisCardHeader.find('.flight-result-input-disabled').length > 2) {
		// Check to see if the user is currently editing another flight. Ask if they want to
		//	abondon those edits or save them
		return $.when(confirmCurrentEdits())
		.then(function(saveSucessful) {
			if (saveSucessful) {
				// Add/remove appropriate classes to enable these inputs
				thisCardHeader.find('.flight-result-input')
					.filter(function() { return !($(this).parent().hasClass('cell-ticket') || $(this).parent().hasClass('cell-total_fee')) })//don't enable the ticket column
					.removeClass('flight-result-input-disabled')
				thisCardHeader.siblings().find('.landing-result-input').removeClass('landing-result-input-disabled');
				thisCardHeader.siblings().find('.delete-landing-button, .add-landing-button').removeClass('hidden');
				thisCardHeader.find('.delete-flight-button').removeClass('hidden'); // Show delete buttons
				thisCardHeader.find('.edit-button').addClass('white-haloed');

				let thisCardLink = thisCardHeader.find('.card-link');
				if (thisCardLink.hasClass('collapsed')) thisCardLink.click();
			}
		})
	} else {
		// The user is already editing this flight and wants to stop 
		if (thisCard.hasClass('flight-data-dirty')) {
			if (confirm(`You have unsaved edits. Click 'OK' to save them or 'Cancel' to discard them`)) {
				saveEditsAndDisable(thisCard);
			} else {
				deferred = $.when(discardEdits($(this).closest('.card')))
							.then((saveSucessful) => {
								unfocusEditButton(saveSucessful, thisCard, thisCardHeader);
							})
			}
		} else {
			deferred = $.when(unfocusEditButton(true, thisCard, thisCardHeader));
		}
	}

	return deferred;
}


function deleteFlightFromDB(flightID) {
	$.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'landingsAdminQuery', dbname: 'scenic_landings', queryString: `DELETE FROM flights WHERE id=${flightID};`},
        cache: false,
        success: function(queryResultString){
        	let resultString = queryResultString.trim();
        	if (resultString.startsWith('ERROR') || resultString === "false") {
        		alert('Unabled to delete the track from the database because ' + 
        			resultString.replace('["query returned an empty result"]', '')
        				.replace('ERROR:  ', '')
    			);
        	}

        	hideLoadingIndicator();
        }
    })
}

function deleteFlight(flightCard) {

	if (!confirm('Are you sure you want to delete this flight? This action cannot be undone')) return;
	
	// Remove the card
	flightCard.hide(1500, function(){ 
		
		$(this).remove() 
		const flightID = flightCard.attr('id').replace('flight-card-', '');

		// Delete the flight from the DB
		if (!flightCard.hasClass('provisional')) {
			deleteFlightFromDB(flightID);
		}
		// Recalculate the sum of all fees
		recalcTotalFee($('.card-header').first());

		$('#save-edits-button').addClass('hidden')
    });

}


function onDeleteLandingClick() {
	let thisRow = $(this).closest('tr');
	let thisCardHeader = thisRow.closest('.card').find('.card-header')
	thisRow.hide(400, function() { 
		if (thisRow.hasClass('provisional')) {
			thisRow.remove();
		} else {
			thisRow.addClass('to-delete');
			$(this).closest('.card').addClass('flight-data-dirty');
			$('#save-edits-button').removeClass('hidden');
		}
		recalcTotalFee(thisCardHeader);
	})
}


function onAddNewFlightClick() {

	// Check if there's a new flight that has not yet been saved
	if ($('.card.provisional').length) {
		alert('You already added a new flight that you have not yet saved. You have to save or delete this one before adding another one.')
		return;
	}

	// Ask the user to save or discard edits if there are any
	$.when(confirmCurrentEdits())
	.then( function() {
		let newFlight = cloneableFlight
			.clone(deepWithDataAndEvents=true)
			.appendTo('#result-table .accordion')
			.removeClass('cloneable')
			.hide()
			.slideToggle('fast', function() {
				$(this).find('.edit-button').triggerHandler('click')//.click()
				// THis is pretty lame, but without delaying adding classes, confirmCurrentEdits gets tripped up
				setTimeout(() => {
					$(this).addClass('provisional').addClass('flight-data-dirty')
					$(this).find('.cell-ticket > .flight-result-input')
						.removeClass('flight-result-input-disabled') // This should be editable in case the user wants to add the flight to another ticket
						.attr('type', 'number')
					$('#save-edits-button').removeClass('hidden');
				}, 500)
			} )

		newFlight.find('.flight-result-input')
			.each(function() {
				$(this).val('');
			})
		
		updateCardIDs(newFlight, 'cloned')
		newFlight.find('.cell-ticket > .flight-result-input')
			.removeClass('flight-result-input-disabled') // This should be editable in case the user wants to add the flight to another ticket
			.attr('type', 'number')
			.removeClass('truncatable');
		
	})

}


function addNewLanding(cardElement) {

	let tbody = cardElement.find('.landings-table > tbody');

	if (tbody.find('.provisional').length) {
		if (confirm(`You haven't saved the last landing you added. To replace it with a new landing, click "OK".`  +
			'Otherwise, press "Cancel" and then save your changes.')) {
			tbody.find('.provisional').remove();
		} else {
			return;
		}
	}

	let newRow = cloneableLanding
		.clone(deepWithDataAndEvents=true)
		.appendTo(tbody)

	//recalcTotalFee(cardElement.find('.card-header'));

	cardElement.addClass('flight-data-dirty');
	$('#save-edits-button').removeClass('hidden');
}


function recalcTotalFee(cardHeader) {
	/* 
	helper function to recalculate the fee for this flight when user changes 
	an influencing input (i.e., landing_passengers or landing_type)
	*/
	let feePerPax = cardHeader.find('.cell-fee_passenger > .flight-result-input')//can't use $(this) since 'this' might be n_passengers cell
		.val().replace('$', '')
	let flightTotalInput = cardHeader.find('.cell-total_fee > .flight-result-input')
	let paxCount = 0;
	cardHeader.siblings()//the card body with the table is the only sibling
		.find('.landing-table-row').each(function() {
			if (!$(this).hasClass('to-delete')) { //skip rows the user provisionally deleted
				$(this).find('.cell-landing_passengers.landing-type-scenic > .landing-result-input, .cell-landing_passengers.landing-type-dropoff > .landing-result-input')
					.each(function() {
						paxCount += stringToNumber($(this).val())
					})
			}
		})
	if (isNaN(feePerPax) || isNaN(paxCount)) {
		return;
	}
	let newTotal = (feePerPax * paxCount).toFixed(2);
	flightTotalInput.val('$' + newTotal);

	// Recalc the sum of all fees
	let totalFee = 0;
	cardHeader.closest('.accordion').find('.card-header .cell-total_fee > .flight-result-input').each(
		function() {
			//if ($(this).parent().hasClass('sum-row-cell')) return;
			totalFee += parseFloat($(this).val().replace('$', ''));
		}
	)
	$('#result-footer-row').find('.cell-total_fee').text('$' + totalFee.toFixed(2));

}


async function showQueryResult(selectedAnchor=false) {

	$('#result-header-row').empty();
	$('#place-holder').css('display', 'none');
	$('#result-table-body').empty();
	$('.result-table-footer').remove();
	$('.add-flight-container').remove();

	var landingColumnRow = '';
	for (columnID in landingQueryResult.fields.landings) {
		let fieldName = landingQueryResult.fields.landings[columnID] //column.replace(/[\W]+/g, '_');
		landingColumnRow += `<th class="landing-table-column-header" id="column-${columnID}">${fieldName}</th> `
	}
	landingColumnRow += 
	`<th class="landing-table-column-header cell-landing-button" id="column-landing-buttons">
		<div class="add-landing-container">
			<div class="add-landing-button slide-up-on-hover hidden"><span style="font-size: 35px; margin-top: -8px;">+</span> landing</div>
		</div>
	</th>`

	let displayLandingTypes = {};
	$(`.query-input-container input[type='checkbox']`).each(function() {
		let landingType = $(this).attr('id').replace('checkmark-', '')
		//// need to add function to change the checked vs not check prop
		displayLandingTypes[landingType] = $(this).get(0).checked;
	})

	let locationOptions = '';
	//for 

	var sums = {};
	for (id in landingQueryResult.data) {
		var thisFlight = landingQueryResult.data[id];
		var tableCells = '';
		for (i in flightColumns) {
			let fieldName = flightColumns[i];
			let columnID = getSanitizedFieldName(fieldName);
			let style = ''//column.includes(' notes') ? 'style="width: 30%"' : ""
			//let columnID = //column.replace(/[\W]+/g, '_');
			if (!$(`#column-${columnID}`).length) {
				$(`<div class="result-table-column-header" id="column-${columnID}" ${style}>${fieldName}</div>`)
					.appendTo('#result-header-row')
			}
			//input class="query-input" type="date" id="input-end_date" value="2019-01-01"
			var thisValue = thisFlight[columnID];
			let cellValue = ['total fee', 'fee/passenger'].includes(fieldName) ? 
				'$' + parseFloat(
						thisValue == null ? 0 : thisValue
					).toFixed(2) : 
				thisValue;
			let inputType = 'text';
			if (fieldName.endsWith(' date')) inputType = 'date';
			if (fieldName.endsWith(' time')) inputType = 'time'; 

			tableCells += 
				`<div class="result-table-cell cell-${columnID}" id="result-table-${id}-${columnID}">
					<input class="truncatable flight-result-input flight-result-input-disabled" type="${inputType}" id="result-input-${id}-${columnID}" data-column-id="${columnID}" value="${cellValue}">
				</div>
				`;

			thisValue = !isNaN(parseFloat(thisValue)) && isFinite(thisValue) ? parseFloat(thisValue) : '';
			sums[columnID] = sums[columnID] == undefined ? thisValue : sums[columnID] + thisValue;
		}

		let landingRows = '';
		for (i in thisFlight.landingsOrder) {
			let landingID = thisFlight.landingsOrder[i];
			let thisLanding = thisFlight.landings[landingID];
			let thisRow = '';
			let landingType = thisLanding.landing_type;
			for (i in landingColumns) {
				let fieldName = landingColumns[i];
				let columnID = getSanitizedFieldName(fieldName);
				//let columnID = column.replace(/[\W]+/g, '_');
				let inputHTML;
				if (columnID === 'landing_location' || columnID === 'landing_type') {
					inputHTML = 
					`<select class="landing-result-input landing-result-input-disabled" id="landing-input-${landingID}-${columnID}" data-column-id="${columnID}" value="${thisLanding[columnID]}">
						${columnID === 'landing_location' ? landingLocationOptions : landingTypeOptions}
					</select>`
				} else if (columnID === 'landing_passengers') {
					inputHTML = `<input class="truncatable landing-result-input landing-result-input-disabled" type="number" id="landing-input-${landingID}-${columnID}" data-column-id="${columnID}" value="${thisLanding[columnID]}">`
				} else {
					inputHTML = `<input class="truncatable landing-result-input landing-result-input-disabled" type="text" id="landing-input-${landingID}-${columnID}" data-column-id="${columnID}" value="${thisLanding[columnID]}">`
				}

				thisRow += 
					`<td class="landing-table-cell cell-${columnID} landing-type-${landingType}" id="landing-cell-${landingID}-${columnID}" data-column-id="${columnID}">
						${inputHTML}
					</td>`;
					/*`<td class="landing-table-cell cell-${columnID}"> 
						<span class="truncatable" style="width: 100%">${thisLanding[column]}</span> 
					</td>`*/
			}
			// Add delete button
			thisRow += 
				`<td class="landing-table-cell cell-landing-button landing-type-${landingType}">
					<button class="delete-landing-button hidden slide-up-on-hover" id="delete-${id}"></button>
				</td>`

			let displayClass = displayLandingTypes[landingType] ? '' : 'hidden'
			landingRows += `<tr class="landing-table-row ${displayClass}" data-landing-id=${landingID}>${thisRow}</tr>`;
		}
		//
		$(`
			<div class="card" id="flight-card-${id}"> 
				<div class="card-header max-0 px-0" id="cardHeader-${id}" style="width:100%; position: relative;">
					<div class="row result-table-row">
						<div class="flight-cell-container">
							${tableCells}
						</div>
						<div class="flight-row-button-container">
							<button class="delete-flight-button hidden slide-up-on-hover" id="delete-${id}"></button>
							<button class="edit-button slide-up-on-hover" id="edit-${id}"></button>
						</div>
					</div>
					<a class="collapsed card-link" data-toggle="collapse" href="#cardContent-${id}" style="width: 100%; height:100%">
						<div class="row anchor-content">
							<i class="fa fa-chevron-down pull-right"></i>
						</div>
					</a>
				</div>
				<div class="collapse" id="cardContent-${id}" aria-labeledby="cardHeader-${id}" data-parent="result-table-body" style="width: 100%; padding-left: 5%;">
					<div class="landing-content-container">
						<table class="landings-table">
							<thead>
								<tr>${landingColumnRow}</tr>
							</thead>
							<tbody>
								${landingRows}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		`)
		.appendTo('#result-table-body')
		

	}

	$('#result-header-row').parent().append(
		`<div class="result-table-column-header add-flight-container">
			<div class="add-flight-button slide-up-on-hover"><span>+</span> flight</div>
		</div>
		`
	)
	$('.add-flight-button').click(onAddNewFlightClick)


	$('.card-header').click(function(e) {

		//let jqObject = $(this).removeClass('flight-result-input-disabled')
		let target = $(e.target)
		if (target.find('.flight-result-input-disabled').length) {
			$(this).find('a').click()
			//$('.card-header > a').get(0).click()
		}
	})

	// Set value of landing selects because for some reason, setting the value in the HTML doesn't work
	$('.landing-table-cell > select').each(function() {
		let flightID = $(this).closest('.card').attr('id').replace('flight-card-', '');
		let landingID = $(this).closest('tr').attr('data-landing-id');
		let columnID = $(this).attr('data-column-id');
		$(this).val(landingQueryResult.data[flightID].landings[landingID][columnID])
	})

	$('.edit-button').click(onEditButtonClick);

	if (!editors.includes(username.toLowerCase())) {
		$('.edit-button, .add-flight-button, .save-button').addClass('hidden')
	}

	$('.delete-flight-button').click(function() {
		deleteFlight($(this).closest('.card'));
	})

	$('.delete-landing-button').click(onDeleteLandingClick)

	$('.add-landing-button').click(function() {
		addNewLanding($(this).closest('.card'));
	})

	// On focus event for inputs
	$('.flight-result-input, .landing-result-input')
		.focus( function() {
			// When a flight input 
			let thisParent = $(this).closest('.card-header').parent();
			if (!thisParent.find('.collapse.show').length) {
				thisParent.find('a').click();
			}

			// Show the modal expanded text if this textbox is truncated (and the modal is not already shown)
			if ($(this).hasClass('truncated') && $('.modal-content').length === 0) {
				showExpandedText($(this).parent().attr('id'));
				const thisVal = $('.modal-expanded-text').val();
				$('.modal-expanded-text').val(thisVal);
			}

			// If there's already a modal content div being shown, give the focus to it. This only happens 
			//when the modal content is shown and then the user switches to another tab/window and back
			if ($('.modal-content').length) {
				$('.modal-expanded-text').focus();
			}
		})

	// When an input value is changed
	$('.flight-result-input, .landing-result-input')
		.change( function() {
			let thisCard = $(this).closest('.card');
			let thisCardHeader = thisCard.find('.card-header');
			let thisFlightID = thisCardHeader.attr('id').replace('cardHeader-', '')
			if (!editedFlights.includes(thisFlightID)) {
				editedFlights.push(thisFlightID);
			}
			// add class to indicate that this flight is in a dirty state
			thisCard.addClass('flight-data-dirty')
			$('#save-edits-button').removeClass('hidden');
			
			// Add the expandText button for any textboxes that are truncated
			thisCardHeader.closest('.card').find('.truncatable:truncated').each(function(){
				addExpandTruncatedButton($(this).parent(), $(this));
			})

			// If this is the landing_type field, change the landing-type class
			let parentCell = $(this).parent();
			if (parentCell.hasClass('cell-landing_type')) {
				let siblingCells = $(this).closest('tr').find('.landing-table-cell')
				siblingCells
					.removeClass('landing-type-scenic')
					.removeClass('landing-type-dropoff')
					.removeClass('landing-type-pickup');
				const landingType = $(this).val();
				siblingCells.addClass('landing-type-' + landingType)
			}

			// If this is the fee/pax field or the landing type, update the total fee
			if (parentCell.hasClass('cell-fee_passenger') || parentCell.hasClass('cell-landing_passengers') || parentCell.hasClass('cell-landing_type')) {
				recalcTotalFee(thisCardHeader);
			}

		})

	// Round minute to nears 15 minute interval 
	$('.cell-departure_time	> .flight-result-input').focusout(function(){
		let hours, minutes;
		[hours, minutes] = $(this).val().split(':')
		let roundedMinutes = ('0' + ((Math.round(parseInt(minutes)/15) * 15) % 60)).slice(-2)
		$(this).val(`${hours}:${roundedMinutes}`)
	})

	// Expand on double-click (and show help tip to indicate this)
	/*$('td').dblclick(function() {
		$(this).css('white-space',
			$(this).css('white-space') === 'nowrap' ? 'normal' : 'nowrap')
	})
	.hover(function() {
		if (Math.ceil($(this).outerWidth()) < $(this)[0].scrollWidth) {
			$(this).attr('title', 'Double-click to expand full text')
		} else {
			$(this).attr('title', '')
		}
	});*/

	var sumRow = '';
	for (columnID in sums) {

		//let columnID = column.replace(/[\W]+/g, '_');
		let fieldName = landingQueryResult.fields.flights[columnID]
		if (fieldName === 'total fee' || fieldName.includes('dropoff')){
			sumRow += `<div class="result-table-cell cell-${columnID}">${fieldName === 'total fee' ? '$' + sums[columnID].toFixed(2) : sums[columnID]}</div>`;
		} else {
			sumRow += `<div class="result-table-cell cell-${columnID}"></div>`
		}
	}

	$(`
		<div class="row result-table-header result-table-footer" id="sum-row">
			<div class="result-header-cell-container sum-row-cell" id="result-footer-row">
				${sumRow}
			</div>
		</div>
	`).appendTo('#flight-result-container')	

	// Set the width for each cell to be the same as it's column
	resizeColumns();

	// Make a cloneable (hidden) flight and a cloneable landing for each card
	cloneableLanding = 
		$('.card').find('.landing-table-row')
			.last()
			.clone(deepWithDataAndEvents=true)
			.attr('data-landing-id', '')
			.addClass('provisional');
	cloneableLanding.find('.landing-result-input-disabled').removeClass('landing-result-input-disabled');
	cloneableLanding.find('.delete-landing-button').removeClass('hidden');
	cloneableLanding.find('.landing-table-cell > .landing-result-input').each(function() {$(this).val(null)});
	cloneableFlight = 
		$('#result-table').find('.card')
			.last()
			.clone(deepWithDataAndEvents=true)
			.attr('id', 'flight-card-cloned')
			.addClass('cloneable');
	cloneableFlight.find('.card-header').attr('id', 'cardHeader-cloned');
	cloneableFlight.find('.landings-table > tbody')
		.empty()
		.append(cloneableLanding)

	// Show the export button
	$('#export-button-container').css('display', 'flex');
}	


function onCheckboxClick() {
	
	// If a query has been run, show/hide the associated landing type
	if (Object.keys(landingQueryResult).length) {
		/*var selectedAnchor = false;
		$('.card').find('a').each(function() {
			if (!$(this).hasClass('collapsed')) {
				selectedAnchor = $(this)
			}
		})*/
		showQueryResult(landingQueryResult);
	}
}


function showLoadingIndicator() {

    //set a timer to turn off the indicator after a max of 15 seconds because 
    //  sometimes hideLoadingIndicator doesn't get called or there's some mixup 
    //  with who called it
    setTimeout(hideLoadingIndicator, 15000);

    var thisCaller = showLoadingIndicator.caller.name;

	var indicator = $('#loading-indicator').css('display', 'block')
	$('#loading-indicator-background').css('display', 'block');

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
        $('#loading-indicator-background').css('display', 'none');
        indicator.css('display', 'none');
    }

}


function onExportDataClick() {

	// If there are unsaved edits, ask if the user wants to save them
	if ($('.flight-data-dirty').length) {
		if (confirm(`You have unsaved edits. Click 'OK' to save them or 'Cancel' to discard them`)) {
			saveEdits($('.flight-data-dirty').attr('id').replace('flight-card-', ''))
		} 
	}

	// Save query result to disk
	const operator = landingQueryResult.data[Object.keys(landingQueryResult.data)[0]].operator_code.toLowerCase();
	const filename = `landing_fees_${operator}_${$('#input-start_date').val()}_${$('#input-end_date').val()}.csv`

	let csvString = $.csv.fromObjects(landingQueryResult.data)
	let a = $(`<a href="data:text/plain;charset=utf-8,${encodeURIComponent(csvString)}" download="${filename}"></a>`)
		.appendTo('body')
		.get(0).click(); // need to trigger the native dom click event because jQuery excludes it
	$(a).remove();

}


//code from https://codepen.io/sosuke/pen/Pjoqqp

'use strict';

class Color {
	constructor(r, g, b) {
		this.set(r, g, b);
	}
	
	toString() {
		return `rgb(${Math.round(this.r)}, ${Math.round(this.g)}, ${Math.round(this.b)})`;
	}

	set(r, g, b) {
		this.r = this.clamp(r);
		this.g = this.clamp(g);
		this.b = this.clamp(b);
	}

	hueRotate(angle = 0) {
		angle = angle / 180 * Math.PI;
		const sin = Math.sin(angle);
		const cos = Math.cos(angle);

		this.multiply([
			0.213 + cos * 0.787 - sin * 0.213,
			0.715 - cos * 0.715 - sin * 0.715,
			0.072 - cos * 0.072 + sin * 0.928,
			0.213 - cos * 0.213 + sin * 0.143,
			0.715 + cos * 0.285 + sin * 0.140,
			0.072 - cos * 0.072 - sin * 0.283,
			0.213 - cos * 0.213 - sin * 0.787,
			0.715 - cos * 0.715 + sin * 0.715,
			0.072 + cos * 0.928 + sin * 0.072,
		]);
	}

	grayscale(value = 1) {
		this.multiply([
			0.2126 + 0.7874 * (1 - value),
			0.7152 - 0.7152 * (1 - value),
			0.0722 - 0.0722 * (1 - value),
			0.2126 - 0.2126 * (1 - value),
			0.7152 + 0.2848 * (1 - value),
			0.0722 - 0.0722 * (1 - value),
			0.2126 - 0.2126 * (1 - value),
			0.7152 - 0.7152 * (1 - value),
			0.0722 + 0.9278 * (1 - value),
		]);
	}

	sepia(value = 1) {
		this.multiply([
			0.393 + 0.607 * (1 - value),
			0.769 - 0.769 * (1 - value),
			0.189 - 0.189 * (1 - value),
			0.349 - 0.349 * (1 - value),
			0.686 + 0.314 * (1 - value),
			0.168 - 0.168 * (1 - value),
			0.272 - 0.272 * (1 - value),
			0.534 - 0.534 * (1 - value),
			0.131 + 0.869 * (1 - value),
		]);
	}

	saturate(value = 1) {
		this.multiply([
			0.213 + 0.787 * value,
			0.715 - 0.715 * value,
			0.072 - 0.072 * value,
			0.213 - 0.213 * value,
			0.715 + 0.285 * value,
			0.072 - 0.072 * value,
			0.213 - 0.213 * value,
			0.715 - 0.715 * value,
			0.072 + 0.928 * value,
		]);
	}

	multiply(matrix) {
		const newR = this.clamp(this.r * matrix[0] + this.g * matrix[1] + this.b * matrix[2]);
		const newG = this.clamp(this.r * matrix[3] + this.g * matrix[4] + this.b * matrix[5]);
		const newB = this.clamp(this.r * matrix[6] + this.g * matrix[7] + this.b * matrix[8]);
		this.r = newR;
		this.g = newG;
		this.b = newB;
	}

	brightness(value = 1) {
		this.linear(value);
	}
	contrast(value = 1) {
		this.linear(value, -(0.5 * value) + 0.5);
	}

	linear(slope = 1, intercept = 0) {
		this.r = this.clamp(this.r * slope + intercept * 255);
		this.g = this.clamp(this.g * slope + intercept * 255);
		this.b = this.clamp(this.b * slope + intercept * 255);
	}

	invert(value = 1) {
		this.r = this.clamp((value + this.r / 255 * (1 - 2 * value)) * 255);
		this.g = this.clamp((value + this.g / 255 * (1 - 2 * value)) * 255);
		this.b = this.clamp((value + this.b / 255 * (1 - 2 * value)) * 255);
	}

	hsl() {
		// Code taken from https://stackoverflow.com/a/9493060/2688027, licensed under CC BY-SA.
		const r = this.r / 255;
		const g = this.g / 255;
		const b = this.b / 255;
		const max = Math.max(r, g, b);
		const min = Math.min(r, g, b);
		let h, s, l = (max + min) / 2;

		if (max === min) {
			h = s = 0;
		} else {
			const d = max - min;
			s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
			switch (max) {
				case r:
					h = (g - b) / d + (g < b ? 6 : 0);
					break;

				case g:
					h = (b - r) / d + 2;
					break;

				case b:
					h = (r - g) / d + 4;
					break;
			}
			h /= 6;
		}

		return {
			h: h * 100,
			s: s * 100,
			l: l * 100,
		};
	}

	clamp(value) {
		if (value > 255) {
			value = 255;
		} else if (value < 0) {
			value = 0;
		}
		return value;
	}
}

class Solver {
	constructor(target, baseColor) {
		this.target = target;
		this.targetHSL = target.hsl();
		this.reusedColor = new Color(0, 0, 0);
	}

	solve() {
		const result = this.solveNarrow(this.solveWide());
		return {
			values: result.values,
			loss: result.loss,
			filter: this.css(result.values),
		};
	}

	solveWide() {
		const A = 5;
		const c = 15;
		const a = [60, 180, 18000, 600, 1.2, 1.2];

		let best = { loss: Infinity };
		for (let i = 0; best.loss > 25 && i < 3; i++) {
			const initial = [50, 20, 3750, 50, 100, 100];
			const result = this.spsa(A, a, c, initial, 1000);
			if (result.loss < best.loss) {
				best = result;
			}
		}
		return best;
	}

	solveNarrow(wide) {
		const A = wide.loss;
		const c = 2;
		const A1 = A + 1;
		const a = [0.25 * A1, 0.25 * A1, A1, 0.25 * A1, 0.2 * A1, 0.2 * A1];
		return this.spsa(A, a, c, wide.values, 500);
	}

	spsa(A, a, c, values, iters) {
		const alpha = 1;
		const gamma = 0.16666666666666666;

		let best = null;
		let bestLoss = Infinity;
		const deltas = new Array(6);
		const highArgs = new Array(6);
		const lowArgs = new Array(6);

		for (let k = 0; k < iters; k++) {
			const ck = c / Math.pow(k + 1, gamma);
			for (let i = 0; i < 6; i++) {
				deltas[i] = Math.random() > 0.5 ? 1 : -1;
				highArgs[i] = values[i] + ck * deltas[i];
				lowArgs[i] = values[i] - ck * deltas[i];
			}

			const lossDiff = this.loss(highArgs) - this.loss(lowArgs);
			for (let i = 0; i < 6; i++) {
				const g = lossDiff / (2 * ck) * deltas[i];
				const ak = a[i] / Math.pow(A + k + 1, alpha);
				values[i] = fix(values[i] - ak * g, i);
			}

			const loss = this.loss(values);
			if (loss < bestLoss) {
				best = values.slice(0);
				bestLoss = loss;
			}
		}
		return { values: best, loss: bestLoss };

		function fix(value, idx) {
			let max = 100;
			if (idx === 2 /* saturate */) {
				max = 7500;
			} else if (idx === 4 /* brightness */ || idx === 5 /* contrast */) {
				max = 200;
			}

			if (idx === 3 /* hue-rotate */) {
				if (value > max) {
					value %= max;
				} else if (value < 0) {
					value = max + value % max;
				}
			} else if (value < 0) {
				value = 0;
			} else if (value > max) {
				value = max;
			}
			return value;
		}
	}

	loss(filters) {
		// Argument is array of percentages.
		const color = this.reusedColor;
		color.set(0, 0, 0);

		color.invert(filters[0] / 100);
		color.sepia(filters[1] / 100);
		color.saturate(filters[2] / 100);
		color.hueRotate(filters[3] * 3.6);
		color.brightness(filters[4] / 100);
		color.contrast(filters[5] / 100);

		const colorHSL = color.hsl();
		return (
			Math.abs(color.r - this.target.r) +
			Math.abs(color.g - this.target.g) +
			Math.abs(color.b - this.target.b) +
			Math.abs(colorHSL.h - this.targetHSL.h) +
			Math.abs(colorHSL.s - this.targetHSL.s) +
			Math.abs(colorHSL.l - this.targetHSL.l)
		);
	}

	css(filters) {
		function fmt(idx, multiplier = 1) {
			return Math.round(filters[idx] * multiplier);
		}
		return `filter: invert(${fmt(0)}%) sepia(${fmt(1)}%) saturate(${fmt(2)}%) hue-rotate(${fmt(3, 3.6)}deg) brightness(${fmt(4)}%) contrast(${fmt(5)}%);`;
	}
}

function hexToRgb(hex) {
	// Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
	const shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
	hex = hex.replace(shorthandRegex, (m, r, g, b) => {
		return r + r + g + g + b + b;
	});

	const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
	return result
		? [
			parseInt(result[1], 16),
			parseInt(result[2], 16),
			parseInt(result[3], 16),
		]
		: null;
}

//export {Color, Solver};

/* To use:
	const color = new Color(rgb[0], rgb[1], rgb[2]);
	const solver = new Solver(color);
	const result = solver.solve();
	const filterString = result.fitler;
*/
