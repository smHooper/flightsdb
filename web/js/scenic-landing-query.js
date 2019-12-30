

//import * as flights from './flights.js';
//const dataSteward = 'dena_flight_data@nps.gov';


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
			flights.departure_datetime::date AS date, 
			to_char(flights.departure_datetime, 'HH:MI AM') AS time, 
			coalesce(flights.scenic_route, '') AS "scenic route",
			concession_fees.n_passengers AS passengers, 
			concession_fees.fee 
		FROM 
			flights 
		INNER JOIN 
			concession_fees ON flights.id = concession_fees.flight_id 
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
	$('tbody').empty();
	if (result.length) {

	} else {
		console.log('result was empty')
		return;
	}

	var columnIndex = 0;
	var columnIndices = {};
	var sums = {};
	for (i in result) {
		var thisFlight = result[i];
		var tableCells = '';
		for (column in thisFlight) {
			let columnID = column.replace(' ', '_');
			if (!$(`#th-${columnID}`).length) {
				$(`<th id="th-${columnID}">${column}</th>`)
					.appendTo('#result-header-row')
				columnIndices[column] = columnIndex;
				columnIndex ++;
			}

			var thisValue = thisFlight[column];
			tableCells += `<td>${column === 'fee' ? '$' + parseFloat(thisValue).toFixed(2) : thisValue}</td>`

			thisValue = !isNaN(parseFloat(thisValue)) && isFinite(thisValue) ? parseFloat(thisValue) : '';
			sums[column] = sums[column] === undefined ? thisValue : sums[column] + thisValue; 
		}
		$(`<tr>${tableCells}</tr>`).appendTo('#result-table > tbody')
	}

	
	var sumRow = '';
	for (column in sums) {
		if (column.includes('passengers') || column.includes('fee') || column.includes('dropoff')){
			sumRow += `<td>${column === 'fee' ? '$' + sums[column].toFixed(2) : sums[column]}</td>`;
		} else {
			sumRow += '<td></td>'
		}
	}
	$(`<tr style="font-weight: bold; border-bottom: none;">${sumRow}</tr>`).appendTo('#result-table > tbody')	



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