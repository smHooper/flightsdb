

<?php

include '../config/track-editor-config.php';


function runQuery($ipAddress, $port, $dbName, $username, $password, $queryStr) {
	/*return result of a postgres query as an array*/
	
	$conn = pg_connect("hostaddr=$ipAddress port=$port dbname=$dbName user=$username password=$password");
	if (!$conn) {
		return false;
	}

	$result = pg_query($conn, $queryStr);
	if (!$result) {
	  	echo pg_last_error();
	}

	return pg_fetch_all($result);
}


if (isset($_POST['action'])) {
	
	// retrieve the names of all files that need to be edited
	if ($_POST['action'] == 'getFiles') {
		$json_files = array_filter(glob('data/*geojsons.json'));
		echo json_encode($json_files);
	}

	// write json data to the server
	if ($_POST['action'] == 'writeFile') {
		// check that both the json string and the path to write the json to were given

		if (isset($_POST['jsonString']) && isset($_POST['filePath'])) {
			$success = file_put_contents($_POST['filePath'], $_POST['jsonString']);
			echo $success;
		} else {
			echo false;
		}
	}

	if ($_POST['action'] == 'getUser') {
		if ($_SERVER['AUTH_USER']) echo preg_replace("/^.+\\\\/", "", $_SERVER["AUTH_USER"]);
    	else echo false;

	}

	if ($_POST['action'] == 'query') {

		if (isset($_POST['queryString'])) {
			$result = runQuery($dbhost, $dbport, $dbname, $readonly_username, $readonly_password, $_POST['queryString']);
			echo json_encode($result);
		} else {
			echo false;
		}
	}
}

?>