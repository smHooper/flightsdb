export function showLoadingIndicator() {

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


export function hideLoadingIndicator(caller) {
    

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


export function fillSelectOptions(selectElementID, queryString, dbname, optionClassName='track-info-option') {
    

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