

//import * as flights from './flights.js';
//const dataSteward = 'dena_flight_data@nps.gov';

const flightColumns = [
	'departure date',
	'departure time',
	'scenic route',
	'total passengers',
	'total fee'
];
const landingColumns = [
	"landing location", 
	"landing passengers", 
	"landing type", 
	"landing notes", 
	"justification"
];

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


function fillSelectOptions(selectElementID, queryString, dbname, optionClassName='track-info-option') {
    

    var deferred = $.ajax({
        url: 'geojson_io.php',
        method: 'POST',
        data: {action: 'query', dbname: dbname, queryString: queryString},
        cache: false,
        success: function(queryResultString){
            var queryResult = queryResultString.startsWith('ERROR') ? false : $.parseJSON(queryResultString);
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


function onRunClick(event) {

	const operator_code = $('#select-operator').val();
	if (!operator_code.length) {
		alert('You must select on operator first before running a query');
		return;
	}

	showLoadingIndicator();

	const start_date = $('#input-start_date').val();
	const end_date = $('#input-end_date').val();
	
	const sql = `
		SELECT 
			flights.id, 
			flights.departure_datetime::date AS "departure date", 
			to_char(flights.departure_datetime, 'HH:MI AM') AS "departure time", 
			coalesce(flights.scenic_route, '') AS "scenic route",
			concession_fees.n_passengers AS "total passengers", 
			concession_fees.fee AS "total fee", 
			landing_locations.name AS "landing location", 
			landings.n_passengers AS "landing passengers", 
			landings.landing_type AS "landing type", 
			coalesce(landings.notes, '') AS "landing notes", 
			coalesce(landings.justification, '') AS justification  
		FROM 
			flights 
		INNER JOIN 
			concession_fees ON flights.id = concession_fees.flight_id 
		INNER JOIN 
			landings ON flights.id = landings.flight_id 
		INNER JOIN 
			landing_locations ON landings.location = landing_locations.code 
		WHERE 
			flights.departure_datetime::date BETWEEN '${start_date}' AND '${end_date}' AND 
			flights.operator_code = '${operator_code}'
		; 
	`;

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
					showQueryResult(queryResult).then(hideLoadingIndicator('onRunClick'));
				}
			} else {
				console.log(`error running query: ${queryResultString}`);
			}
		}
	}).fail((xhr, status, error) => {
		console.log(`query failed with status ${status} because ${error} from query:\n${sql}`)
	});
	event.returnValue = false;
}


async function showQueryResult(result) {

	$('#result-header-row').empty();
	$('#place-holder').css('display', 'none');
	$('#result-table-body').empty();
	//$('#sum-row')

	// Organize the flights into an object with flight attributes and a landing (array) 
	//	property that contains info about each landing
	var flights = {};
	for (i in result) {
		let row = result[i];
		let thisFlightID = row.id;
		if (flights[thisFlightID] === undefined) {
			flights[thisFlightID] = {};
			for (i in flightColumns) {
				let column = flightColumns[i];
				flights[thisFlightID][column] = row[column];
			}
			flights[thisFlightID].landings = [];
		}

		//flights[thisFlightID] = {};


		let thisLanding = {};
		for (i in landingColumns) {
			let column = landingColumns[i];
			thisLanding[column] = row[column];
		}
		flights[thisFlightID].landings.push(thisLanding);

		/*if (flights[thisFlightID].landings === undefined) {
			flights[thisFlightID].landings = [thisLanding];
		} else {
			flights[thisFlightID].landings.push(thisLanding)
		}*/

		/*for (column in row) {
			if (flightColumns.includes(column)) {
				flights[thisFlightID][column] = row[column]
			} else if (landingColumns.includes(column)) {
				if (flights[thisFlightID].landings === undefined) flights[thisFlightID].landings = [];
				let landingIndex = flights[thisFlightID].landings.length - 1;
				let thisLanding = flights[thisFlightID].landings[landingIndex];
				if (thisLanding === undefined) {
					flights[thisFlightID].landings.push({});
					landingIndex ++;
				}
				flights[thisFlightID].landings[landingIndex][column] = row[column];
			}
		}*/
	}

	var landingColumnRow = '';
	for (i in landingColumns) {
		let column = landingColumns[i]
		let columnID = column.replace(' ', '_');
		landingColumnRow += `<th class="landing-table-column-header" id="column-${columnID}">${column}</th> `
	}

	var columnIndex = 0;
	var columnIndices = {};
	var sums = {};
	for (id in flights) {
		var thisFlight = flights[id];
		var tableCells = '';
		for (i in flightColumns) {
			let column = flightColumns[i];
			let columnID = column.replace(' ', '_');
			if (!$(`#column-${columnID}`).length) {
				$(`<div class="result-table-column-header" id="column-${columnID}">${column}</div>`)
					.appendTo('#result-header-row')
				columnIndices[column] = columnIndex;
				columnIndex ++;
			}

			var thisValue = thisFlight[column];
			let cellValue = column === 'total fee' ? '$' + parseFloat(thisValue).toFixed(2) : thisValue;
			tableCells += `<div class="result-table-cell cell-${columnID}">${cellValue}</div>`

			thisValue = !isNaN(parseFloat(thisValue)) && isFinite(thisValue) ? parseFloat(thisValue) : '';
			sums[column] = sums[column] === undefined ? thisValue : sums[column] + thisValue;
		}

		let landingRows = '';
		for (i in thisFlight.landings) {
			let thisLanding = thisFlight.landings[i];
			let thisRow = '';
			for (column in thisLanding) {
				let columnID = column.replace(' ', '_');
				thisRow += `<td class="landing-table-cell cell-${columnID}">${thisLanding[column]}</td>`;
			}
			landingRows += `<tr>${thisRow}</tr>`;
		}

		$(`<div class="card" id="result-row-${id}"> 
				<div class="card-header max-0 px-0" id="cardHeader-${id}" style="width:100%;">
					<a class="collapsed card-link" data-toggle="collapse" href="#cardContent-${id}" style="width: 100%;">
						<div class="row result-table-row">${tableCells}</div>
					</a>
				</div>
				<div class="collapse" id="cardContent-${id}" aria-labeledby="cardHeader-${id}" data-parent="result-table-body" style="width: 100%; padding-left: 5%;">
					<table style="width: 97%; float: right; margin-right: 2.5%; margin-top: 10px; margin-bottom: 30px;">
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

	var sumRow = '';
	for (column in sums) {
		let columnID = column.replace(' ', '_');
		if (column.includes('passengers') || column.includes('fee') || column.includes('dropoff')){
			sumRow += `<div class="result-table-cell cell-${columnID}">${column === 'total fee' ? '$' + sums[column].toFixed(2) : sums[column]}</div>`;
		} else {
			sumRow += `<div class="result-table-cell cell-${columnID}"></div>`
		}
		
		// Set the width for each cell to be the same as it's column
		let columnWidth = $(`#column-${columnID}`).width();
		$('#result-table-body').find(`.result-table-cell.cell-${columnID}`).css('width', columnWidth);
	}
	$(`<div class="row result-table-row" id="sum-row" style="font-weight: bold; border-bottom: none; background-color: #8e5757e6;">${sumRow}</div>`).appendTo('#result-table-body')	



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


