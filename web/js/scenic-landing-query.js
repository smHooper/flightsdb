

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

var landingQueryResult = {}; //global var to store result for writing CSV  
var currentControlVal; // used in .change() events to decide whether function should be fired
var editedFlights = []; // global var to keep track of data that have been edited (stores flight IDs)
var landingTypeOptions = ''; // Store results of query
var landingLocationOptions = [];

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


function onRunClick(event) {

	// prevent the form from resetting
	event.returnValue = false;

	// If there are unsaved edits, ask if the user wants to save them
	if ($('.flight-data-dirty').length) {
		if (confirm(`You have unsaved edits. Click 'OK' to save them or 'Cancel' to discard them`)) {
			saveEdits($('.flight-data-dirty').attr('id').replace('cardHeader-', ''))
		} 
	}

	const operator_code = $('#select-operator').val();
	if (!operator_code.length) {
		alert('You must select on operator first before running a query');
		return;
	}

	const ticketNumbers = $('#select-tickets').val().join(',');
	const ticketSearchClause = (ticketNumbers.length) ? ` AND ticket IN (${ticketNumbers})` : '';

	const locations = $('#select-locations').val().join(`', '`);
	const locationSearchClause = (locations.length) ? ` AND landings.location IN ('${locations}')` : '';

	const start_date = $('#input-start_date').val();
	const end_date = $('#input-end_date').val();
	
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
			concession_fees_view.n_fee_passengers AS "fee_passengers", 
			concession_fees_view.fee AS "total fee", 
			landings.location AS "landing location",
			landings.id AS landing_id,  
			landings.n_passengers AS "landing passengers", 
			landings.landing_type AS "landing type", 
			coalesce(landings.notes, '') AS "landing notes", 
			coalesce(landings.justification, '') AS justification  
		FROM 
			flights 
		INNER JOIN 
			concession_fees_view ON flights.id = concession_fees_view.flight_id 
		INNER JOIN 
			landings ON flights.id = landings.flight_id 
		INNER JOIN 
			landing_locations ON landings.location = landing_locations.code 
		WHERE 
			flights.departure_datetime::date BETWEEN '${start_date}' AND '${end_date}' AND 
			flights.operator_code = '${operator_code}'
			${ticketSearchClause}
			${locationSearchClause}
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
						hideLoadingIndicator('onRunClick');
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
					<img class="save-modal-text-button" src="imgs/save_icon_30px.svg" data-text-source="${truncatable.attr('id')}" style="${filterString}">
				</span>
				<span class="close-modal-text-button" style="color: ${foreColor}">&times;</span>
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
	})
}


function resizeColumns() {

	for (columnID in landingQueryResult.fields.flights) {
		//let columnID = getSanitizedFieldName(flightColumns[i]) //flightColumns[i].replace(/[\W]+/g, '_');
		try {
			//let padding = parseInt($(`#column-${columnID}`).css('padding-left').replace('px', '')) * 2;
			let columnWidth = $(`#column-${columnID}`).outerWidth() //+ padding;
			$('#result-table-body').find(`.result-table-cell.cell-${columnID}`).css('width', columnWidth);
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

function saveEdits(flightID) {

    const _dbColumns = {
    	'flights': {
			'scenic_route':  'scenic_route',
			'flight_notes':  'notes',
			'fee_passenger': 'fee_per_passenger'
    	},
    	'landings': {
    		'landing_location': 	'location', 
			'landing_passengers': 	'n_passengers', 
			'landing_type': 		'landing_type', 
			'landing_notes': 		'notes', 
			'justification': 		'justification'
    	}
    }

    let thisCardHeader = $(`#cardHeader-${flightID}`);
    let dataSaved = false;

	// Check to make sure this doesn't violate the UNIQUE constraint on flight ID
	let departure_datetime = 
		thisCardHeader.find('.cell-departure_date > .flight-result-input').val() + 
		" " + 
		thisCardHeader.find('.cell-departure_time > .flight-result-input').val();
	let dt = new Date(departure_datetime);
	let flightIDString = 
		landingQueryResult.data[flightID].registration + 
		'_' + 
		dt.getUTCFullYear() +
		("0" + (dt.getUTCMonth()+1)).slice(-2) +
	    ("0" + dt.getUTCDate()).slice(-2) +
	    ("0" + dt.getUTCHours()).slice(-2) +
	    ("0" + dt.getUTCMinutes()).slice(-2);

	/*if (flightIDString !== landingQueryResult.data[flightID].flight_id) {
		var duplicated = false;
		var deferred = $.ajax({
	        url: 'geojson_io.php',
	        method: 'POST',
	        data: {action: 'query', dbname: dbname, queryString: `SELECT * FROM flights WHERE flight_id='${flightIDString}';`},
	        cache: false,
	        success: function(queryResultString){
	            if (queryResultString !== '["query returned an empty result"]') {
	            	duplicated = true;
	            } else if (!queryResultString.startsWith('ERROR')) {

	            }
	        }
        });

	    if (duplicated) {
	    	alert('ERROR: There is already a flight with that registration and departure date and time.');
	    	return false;
	    }

	}*/


	// collect values from inputs and landings table
	let edits = {};
	var flightUpdates = [];
	let sqlValues = []; //gets overwritten with each landing statement
	var sqlStatements = []; // array of SQL statement strings for parametizing
	let sqlParameters = []; // array of arrays containing parameters for corresponding sqlStatements
	let paramCount = 1;
	thisCardHeader
		.find('.flight-result-input')
		.filter(function(){
			return !(
				$(this).hasClass('flight-result-input-disabled') ||
				$(this).parent().hasClass('cell-departure_date') ||
				$(this).parent().hasClass('cell-departure_time')
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
			let thisDBColumn = _dbColumns.flights[columnID];
			flightUpdates.push(`${thisDBColumn}=$${paramCount}`);
			sqlValues.push(thisSQLVal);
			paramCount ++;
		});
	
	// Add departure_datetime and update flight ID
	sqlValues.push(departure_datetime);
	flightUpdates.push(`departure_datetime=$${paramCount}`);
	sqlValues.push(flightIDString);
	flightUpdates.push(`flight_id=$${paramCount + 1}`);

	sqlParameters.push(sqlValues);
	sqlStatements.push(`UPDATE flights SET ${flightUpdates.join(', ')} WHERE id=${flightID};`);
	
	thisCardHeader.siblings() // the landing table is in the collapse
		.find('.landing-table-row')
		.each(function() {
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
				let thisDBColumn = _dbColumns.landings[thisColumnID];
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
	$.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'landingsParamQuery', dbname: 'scenic_landings', queryString: sqlStatements, params: sqlParameters},
        cache: false,
        success: function(queryResultString){
        	let resultString = queryResultString.trim();
        	if (resultString.startsWith('ERROR') || resultString === "false") {
        		alert('Unabled to save changes to the database. ' + resultString);
        	}
        }
    })

}


function discardEdits(cardElement) {

	let flightID = cardElement.attr('id').replace('flight-card-', '');
	cardElement.find('.flight-result-input, .landing-result-input').each(function(){
		if ($(this).hasClass('flight-result-input-disabled') || $(this).hasClass('landing-result-input-disabled')) {
			return;
		}
		let columnID = $(this).attr('data-column-id');
		let thisValue = landingQueryResult.data[flightID][columnID];
		$(this).val(thisValue);
	})

	removeExpandTruncatedButtons();

}


function onEditButtonClick() {
	let thisCardHeader = $(this).closest('.card-header');
	// If there are any disabled inputs, they're all disabled and this row is not currently editable
	if (thisCardHeader.find('.flight-result-input-disabled').length > 2) {
		// Check to see if the user is currently editing another flight. Ask if they want to
		//	abondon those edits or save them
		let enabledFlightInputs = $('#result-table').find('.flight-result-input').filter(function(){
			return !$(this).hasClass('flight-result-input-disabled')
		})
		let enabledLandingInputs = $('#result-table').find('.landing-result-input').filter(function(){
			return !$(this).hasClass('landing-result-input-disabled')
		})
		if (enabledFlightInputs.length) {
			// If there are unsaved edits, ask the user if they want to save them
			let previousCardHeader = enabledFlightInputs.closest('.card-header')
			if (previousCardHeader.hasClass('flight-data-dirty')) {
				let response = confirm(`You are currently editing another flight. Click 'OK' to save them or 'Cancel' to discard them.`);
				if (response) {
					saveEdits(previousCardHeader.attr('id').replace('cardHeader-', ''));
				} else {
					discardEdits(enabledFlightInputs.closest('.card'));
				}
				enabledFlightInputs.closest('.card-header').removeClass('flight-data-dirty');
			}
			enabledFlightInputs.addClass('flight-result-input-disabled');
			enabledLandingInputs.addClass('landing-result-input-disabled');
			$('.edit-button').removeClass('white-haloed'); //remove halo from all (the only) active edit button
			$('.delete-flight-button').addClass('hidden');
		}

		// Add/remove appropriate classes to enable these inputs
		thisCardHeader.find('.flight-result-input')
			.filter(function() { return !($(this).parent().hasClass('cell-ticket') || $(this).parent().hasClass('cell-total_fee')) })//don't enable the ticket column
			.removeClass('flight-result-input-disabled')
		thisCardHeader.siblings().find('.landing-result-input').removeClass('landing-result-input-disabled');
		thisCardHeader.find('.delete-flight-button').removeClass('hidden'); // Show delete button
		thisCardHeader.find('.edit-button').addClass('white-haloed');

		let thisCardLink = thisCardHeader.find('.card-link');
		if (thisCardLink.hasClass('collapsed')) thisCardLink.click();
	} else {
		// The user is already editing this flight and wants to stop 
		if (thisCardHeader.hasClass('flight-data-dirty')) {
			if (confirm(`You have unsaved edits. Click 'OK' to save them or 'Cancel' to discard them`)) {
				saveEdits(thisCardHeader.attr('id').replace('cardHeader-', ''));
			} else {
				discardEdits($(this).closest('.card'));
			}
			thisCardHeader.removeClass('flight-data-dirty');
		}
		thisCardHeader.find('.flight-result-input').addClass('flight-result-input-disabled')
		thisCardHeader.siblings().find('.landing-result-input').addClass('landing-result-input-disabled');
		thisCardHeader.find('.delete-flight-button').addClass('hidden');
		thisCardHeader.find('.edit-button').removeClass('white-haloed');
	}
}


function deleteFlight(flightCard) {

	if (!confirm('Are you sure you want to delete this flight? This action cannot be undone')) return;
	
	// Remove the card
	flightCard.hide(1500, function(){ 
		
		$(this).remove() 
		const flightID = flightCard.attr('id').replace('flight-card-', '');

		// Delete the flight from the DB
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

    });
}


async function showQueryResult(selectedAnchor=false) {

	$('#result-header-row').empty();
	$('#place-holder').css('display', 'none');
	$('#result-table-body').empty();

	var landingColumnRow = '';
	for (columnID in landingQueryResult.fields.landings) {
		let fieldName = landingQueryResult.fields.landings[columnID] //column.replace(/[\W]+/g, '_');
		landingColumnRow += `<th class="landing-table-column-header" id="column-${columnID}">${fieldName}</th> `
	}

	let displayLandingTypes = {};
	$(`input[type='checkbox']`).each(function() {
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
			let cellValue = ['total fee', 'fee/passenger'].includes(fieldName) ? '$' + parseFloat(thisValue).toFixed(2) : thisValue;
			let inputType = 'text';
			if (fieldName.endsWith(' date')) inputType = 'date';
			if (fieldName.endsWith(' time')) inputType = 'time'; 

			tableCells += 
				`<div class="result-table-cell cell-${columnID}" id="result-table-${id}-${columnID}">
					<input class="truncatable flight-result-input flight-result-input-disabled" type="${inputType}" id="result-input-${id}-${columnID}" data-column-id="${columnID}" value="${cellValue}">
				</div>
				`;

			thisValue = !isNaN(parseFloat(thisValue)) && isFinite(thisValue) ? parseFloat(thisValue) : '';
			sums[columnID] = sums[columnID] === undefined ? thisValue : sums[columnID] + thisValue;
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
					</select>`
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
			let displayClass = displayLandingTypes[landingType] ? '' : 'hidden'
			landingRows += `<tr class="landing-table-row ${displayClass}" data-landing-id=${landingID}>${thisRow}</tr>`;
		}
		//
		$(`<div class="card" id="flight-card-${id}"> 
				
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
					<table class="landings-table">
						<thead>
							<tr>${landingColumnRow}</tr>
						</thead>
						<tbody>
							${landingRows}
						</tbody>
					</table>
				</div>
			</div>`
		).appendTo('#result-table-body')
	}

	// Add options to selects in landings tables
	landingLocationOptions.appendTo($('.cell-landing_location > .landing-result-input'));
	$(landingTypeOptions).appendTo($('.cell-landing_type > .landing-result-input'));

	$('.card-header').click(function(e) {

		//let jqObject = $(this).removeClass('flight-result-input-disabled')
		let target = $(e.target)
		if (target.find('.flight-result-input-disabled').length) {
			$(this).find('a').click()
			//$('.card-header > a').get(0).click()
		}
		
	})

	$('.edit-button').click(onEditButtonClick);

	$('.delete-flight-button').click(function() {
		deleteFlight($(this).closest('.card'));
	})

	$('.flight-result-input')
		.focus( function() {
			// When a flight input 
			let thisParent = $(this).closest('.card-header').parent();
			if (!thisParent.find('.collapse.show').length) {
				thisParent.find('a').click();
			}

			// Show the modal expanded text if this textbox is truncated (and the modal is not already shown)
			if ($(this).hasClass('truncated') && $('.modal-content').length === 0) {
				showExpandedText($(this).parent().attr('id'));
			}

			// If there's already a modal content div being shown, give the focus to it. This only happens 
			//when the modal content is shown and then the user switches to another tab/window and back
			if ($('.modal-content').length) {
				$('.modal-expanded-text').focus();
			}
		})
	$('.flight-result-input, .landing-result-input')
		.change( function() {
			let thisCardHeader = $(this).closest('.card').find('.card-header');
			let thisFlightID = thisCardHeader.attr('id').replace('cardHeader-', '')
			if (!editedFlights.includes(thisFlightID)) {
				editedFlights.push(thisFlightID);
			}
			// add class to indicate that this flight is in a dirty state
			thisCardHeader.addClass('flight-data-dirty')
			
			// Add the expandText button for any textboxes that are truncated
			thisCardHeader.closest('.card').find('.truncatable:truncated').each(function(){
				addExpandTruncatedButton($(this).parent(), $(this));
			})

			// If this is the fee/pax field, update the total fee
			let parentCell = $(this).parent();
			if (parentCell.hasClass('cell-fee_passenger') || parentCell.hasClass('cell-landing_passengers')) {
				let totalInput = thisCardHeader.find('.cell-total_fee > .flight-result-input')
				let paxCount = 0;
				thisCardHeader.siblings()//the card body with the table is the only sibling
					.find('.cell-landing_passengers.landing-type-scenic > .landing-result-input, .cell-landing_passengers.landing-type-dropoff > .landing-result-input')
					.each(function() {paxCount += stringToNumber($(this).val())})
				let newTotal = (thisCardHeader.find('.cell-fee_passenger > .flight-result-input')//can't use $(this) since 'this' might be n_passengers cell
					.val().replace('$', '') * paxCount).toFixed(2);
				totalInput.val('$' + newTotal);
			}
		})

	// Expand on double-click (and show help tip to indicate this)
	$('td').dblclick(function() {
		$(this).css('white-space',
			$(this).css('white-space') === 'nowrap' ? 'normal' : 'nowrap')
	})
	.hover(function() {
		if (Math.ceil($(this).outerWidth()) < $(this)[0].scrollWidth) {
			$(this).attr('title', 'Double-click to expand full text')
		} else {
			$(this).attr('title', '')
		}
	});

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
			<div class="result-header-cell-container" id="result-footer-row">
				${sumRow}
			</div>
		</div>
	`).appendTo('#result-table-body')	

	// Set the width for each cell to be the same as it's column
	resizeColumns();

	// Show the export button
	$('#export-button-container').css('display', 'flex');

	/*if (selectedAnchor !== false) {
		//$(`${selectedCardID} > a`).get(0).click()
		selectedAnchor.get(0).click()

	}*/
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
			saveEdits($('.flight-data-dirty').attr('id').replace('cardHeader-', ''))
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
