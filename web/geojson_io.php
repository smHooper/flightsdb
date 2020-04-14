

<?php

include '../config/track-editor-config.php';


function runQuery($ipAddress, $port, $dbName, $username, $password, $queryStr, $parameters=array()) {
	/*return result of a postgres query as an array*/

	$conn = pg_connect("hostaddr=$ipAddress port=$port dbname=$dbName user=$username password=$password");
	if (!$conn) {
		return false;
	}

	$result = pg_query_params($conn, $queryStr, $parameters);
	if (!$result) {
	  	echo pg_last_error();
	}

	$resultArray = pg_fetch_all($result) ? pg_fetch_all($result) : array("query returned an empty result");
	return $resultArray;
}


function runQueryWithinTransaction($conn, $queryStr, $parameters=array()) {

	$result = pg_query_params($conn, $queryStr, $parameters);
	if (!$result) {
	  	return pg_last_error();
	}

}


function runCmd($cmd) {
	// can't get this to work for python commands because conda throws
	// an error in conda-script (can't import cli.main)
	$process = proc_open(
		$cmd, 
		array(
			0 => array("pipe", "w"), //STDIN
		    1 => array('pipe', 'w'), // STDOUT
		    2 => array('pipe', 'w')  // STDERR
		), 
		$pipes,
		NULL,
		NULL,
		array('bypass_shell' => true)
	);

	$resultObj; 

	if (is_resource($process)) {

	    $resultObj->stdout = stream_get_contents($pipes[1]);
	    fclose($pipes[1]);

	    $resultObj->stderr = stream_get_contents($pipes[2]);
	    fclose($pipes[2]);

	    $returnCode = proc_close($process);

	    if ($returnCode) {
	    	echo json_encode($resultObj);
	    } else {
	    	echo 'nothing';//false;
	    }
	} else {
		echo json_encode($_SERVER);
	}
}


function deleteFile($filePath) {

	$fullPath = realpath($filePath);

	if (file_exists($fullPath) && is_writable($fullPath)) {
		unlink($fullPath);
		return true;
	} else {
		return false;
	}
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
			$result = runQuery($dbhost, $dbport, $_POST['dbname'], $readonly_username, $readonly_password, $_POST['queryString']);
			echo json_encode($result);
		} else {
			echo "php query failed";//false;
		}
	}

	if ($_POST['action'] == 'landingsAdminQuery') {

		if (isset($_POST['queryString'])) {
			$result = runQuery($dbhost, $dbport, $_POST['dbname'], $landings_admin_username, $landings_admin_password, $_POST['queryString']);
			echo json_encode($result);
		} else {
			echo "php query failed";//false;
		}
	}

	if ($_POST['action'] == 'landingsParamQuery') {

		if (isset($_POST['queryString']) && isset($_POST['params'])) {
			// If there are multiple SQL statements, execute as a single transaction
			if (gettype($_POST['queryString']) == 'array') {
				$resultArray = array();
				$dbname = $_POST['dbname'];
				$conn = pg_connect("hostaddr=$dbhost port=$dbport dbname=$dbname user=$landings_admin_username password=$landings_admin_password");
				if (!$conn) {
					echo "Could not connect DB";
					exit();
				}

				// Begin transations
				pg_query($conn, 'BEGIN');

				for ($i = 0; $i < count($_POST['params']); $i++) {
					// Make sure any blank strings are converted to nulls
					$params = $_POST['params'][$i];
					for ($j = 0; $j < count($params); $j++) {
						if ($params[$j] === '') {
							$params[$j] = null;
						}
					}
					$result = runQueryWithinTransaction($conn, $_POST['queryString'][$i], $params);
					if (strpos($result, 'ERROR') !== false) {
						// roll back the previous queries
						pg_query($conn, 'ROLLBACK');
						echo $result, " from the query $i ", $_POST['queryString'][$i], ' with params ', json_encode($params);
						exit();
					}
				}

				// COMMIT the transaction
				pg_query($conn, 'COMMIT');
				echo "success";

			} else {
				$params = $_POST['params'];
				for ($j = 0; $j < count($params); $j++) {
					if ($params[$j] === '') {
						$params[$j] = null;
					}
				}
				$result = runQuery($dbhost, $dbport, $_POST['dbname'], $landings_admin_username, $landings_admin_password, $_POST['queryString'], $params);
				
				echo json_encode($result);	
			}
		} else {
			echo "php query failed";//false;
		}
	}

	if ($_POST['action'] == 'importData') {
		if (isset($_POST['geojsonString']) && isset($_POST['trackInfoString'])) {
			$geojson = $_POST['geojsonString'];
			$trackInfo = $_POST['trackInfoString'];
			$stderrPath = $_POST['stderrPath'];
			$cmd = "conda activate overflights && python ..\\scripts\\import_from_editor.py $geojson $trackInfo $import_param_file 2> $stderrPath && conda deactivate";
			echo shell_exec($cmd);
		}
	}

	if ($_POST['action'] == 'readTextFile') {
		if (isset($_POST['textPath'])) {
			echo file_get_contents($_POST['textPath']);
		}
	}

	if ($_POST['action'] == 'deleteFile') {
		if (isset($_POST['filePath'])) {
			echo deleteFile($_POST['filePath']) ? 'true' : 'false';
			echo $_POST['filePath'];
		} else {
			echo 'filepath not set or is null';
		}
	}
}

?>