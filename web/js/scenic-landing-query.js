

//import * as flights from './flights.js';
//const dataSteward = 'dena_flight_data@nps.gov';

const flightColumns = [
	'departure date',
	'departure time',
	'scenic route',
	'total passengers',
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


// jquery pseudo-selector to determine if ellipsis is active
$.expr[':'].truncated = function(jqObject) {
	if (Math.ceil($(jqObject).outerWidth()) < $(jqObject)[0].scrollWidth) {
		console.log($(jqObject).attr('id'))
		console.log((Math.ceil($(jqObject).outerWidth()) < $(jqObject)[0].scrollWidth))
	}
	return (Math.ceil($(jqObject).outerWidth()) < $(jqObject)[0].scrollWidth);

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

	// prevent the form from resetting
	event.returnValue = false;

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
			coalesce(flights.notes, '') AS "flight notes", 
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
					showQueryResult(queryResult).then(() => {
						hideLoadingIndicator('onRunClick');
						// Show the first flight's landings after a brief delay
						setTimeout(() => {$('.card-header > a').get(0).click()}, 300);
					})
					landingQueryResult['operator'] = $('#select-operator option:selected').text();
					landingQueryResult['data'] = [...queryResult];//{...queryResult};
				}
			} else {
				console.log(`error running query: ${queryResultString}`);
			}
		}
	}).fail((xhr, status, error) => {
		console.log(`query failed with status ${status} because ${error} from query:\n${sql}`)
	});
}


function showExpandedText(event, cellID) {

	// Prevent the 
	event.stopPropagation();

	let thisCell = $('#' + cellID);
	let truncatable = thisCell.find('.truncatable');
	let text = truncatable.text();
	let position = truncatable.
	$(`
		<div class="expanded-text">${text}</div>
	`)
	//thisCell.append()

}

function resizeColumns() {

	for (i in flightColumns) {
		let columnID = flightColumns[i].replace(' ', '_');
		try {
			//let padding = parseInt($(`#column-${columnID}`).css('padding-left').replace('px', '')) * 2;
			let columnWidth = $(`#column-${columnID}`).outerWidth() //+ padding;
			$('#result-table-body').find(`.result-table-cell.cell-${columnID}`).css('width', columnWidth);
		} catch {
			continue;
		}
	}

	// find any divs truncated because their too long and add a button to show the full text
	/*$('.result-table-cell > .truncatable:truncated').each(function(){
		//console.log(this)
		//if ($(this).hasClass('result-table-cell')) {
		let thisCell = $(this).parent();
		$(`
			<button class="expand-truncated-button">
				<h4 style="color: white;">+</h4>
			</button>
		`).click((event) => {showExpandedText(event, $(thisCell).attr('id'))})
		.appendTo(thisCell);
		//$(thisCell).append(buttonHTML)
		$(this).addClass('truncated');
		//}

	});*/
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

		let thisLanding = {};
		for (i in landingColumns) {
			let column = landingColumns[i];
			thisLanding[column] = row[column];
		}
		flights[thisFlightID].landings.push(thisLanding);
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
			let style = column.includes(' notes') ? 'style="width: 30%"' : ""
			let columnID = column.replace(' ', '_');
			if (!$(`#column-${columnID}`).length) {
				$(`<div class="result-table-column-header" id="column-${columnID}" ${style}>${column}</div>`)
					.appendTo('#result-header-row')
				columnIndices[column] = columnIndex;
				columnIndex ++;
			}

			var thisValue = thisFlight[column];
			let cellValue = column === 'total fee' ? '$' + parseFloat(thisValue).toFixed(2) : thisValue;
			tableCells += 
				`<div class="result-table-cell cell-${columnID}" id="result-table-${id}-${columnID}">
					<div class="truncatable">${cellValue}</div>
				</div>
				`

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
		//
		$(`<div class="card" id="result-row-${id}"> 
				<div class="card-header max-0 px-0" id="cardHeader-${id}" style="width:100%;">
					<a class="collapsed card-link" data-toggle="collapse" href="#cardContent-${id}" style="width: 100%;">
						<div class="row result-table-row">${tableCells}</div>
						<i class="fa fa-chevron-down pull-right"></i>
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
	}

	$(`
		<div class="row result-table-header" id="sum-row" style="font-weight: 500; border-bottom: none; background-color: #8e5757e6; height: 40px;">${sumRow}</div>
	`).appendTo('#result-table-body')	

	// Set the width for each cell to be the same as it's column
	resizeColumns();

	// Show the export button
	$('#export-button-container').css('display', 'flex');
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
	// Save query result to disk

	const filename = `landing_fees_${landingQueryResult['operator'].replace(' ', '_').toLowerCase()}_${$('#input-start_date').val()}_${$('#input-end_date').val()}.csv`

	let csvString = $.csv.fromObjects(landingQueryResult.data)
	let a = $(`<a href="data:text/plain;charset=utf-8,${encodeURIComponent(csvString)}" download="${filename}"></a>`)
		.appendTo('body')
		.get(0).click(); // need to trigger the native dom click event because jQuery excludes it
	$(a).remove();

}
