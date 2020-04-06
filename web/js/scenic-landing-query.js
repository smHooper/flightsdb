

//import * as flights from './flights.js';
//const dataSteward = 'dena_flight_data@nps.gov';

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
	let modalHeight = truncatable.css('line-height').replace('px', '') * nLines
	let truncatableStyle = 
		`
		background-color: ${thisBackgroundColor}; 
		color: ${truncatable.css('color')};
		height: ${modalHeight + 30}px;
		`;
	$(`
		<div class="modal-background"></div>
		<div class="modal-content" style="${truncatableStyle}" data-parent="${truncatable.attr('id')}">
			<div style="width: 100%; height: 24px;">

				<span class="close-modal-text-button">&times;</span>
			</div>
			<div class="row mx-0 px-0" style="display: flex; justify-content: center; width: 100%; height: ${modalHeight}px;">
				<textarea class="modal-expanded-text" style="width: 100%; height: ${modalHeight}px;">${text}</textarea>
			</div>
		</div>
	`).appendTo('body');
	
	/*
				<div class="save-modal-text-button-container">
					<i class="fa fa-save"></i>
				</div>
				<span class="save-modal-text-button-container" data-text-source="${truncatable.attr('id')}"><img class="save-modal-text-button"></span>
	*/

	$('.modal-background, .close-modal-text-button').click(removeModalExpandedText);

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
	});//*/

	/*$('td:truncated').each(function(){
		addExpandTruncatedButton($(this), $(this));

	});*/

	// find any divs that were truncated, but are now fully visible and remove the expand button
	$('.result-table-cell > .truncatable:extended').each(function(){
		let thisCell = $(this).parent();
		thisCell.find('.expand-truncated-button').remove();
	})
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

			if ($(this).hasClass('truncated')) {
				showExpandedText($(this).parent().attr('id'));
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


