

<?php

ini_set('display_errors', 1);
error_reporting(E_ALL);

include '../../config/track-editor-config.php';


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
    $pipes = array();
    $spec = array(
        0 => array("pipe", "r"),
        1 => array("pipe", "w"),
        2 => array("pipe", "w"),
    );

    $process = proc_open($cmd, $spec, $pipes, NULL, NULL);

    if (!is_resource($process)) {
        return array(
            "stdout" => null,
            "stderr" => null,
            "returnCode" => null,
            "is_resource" => false,
        );
    }

    fclose($pipes[0]); // close STDIN immediately since we never write to it

    $stdout = stream_get_contents($pipes[1]);
    fclose($pipes[1]);

    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);

    $returnCode = proc_close($process);

    return array(
        "stdout" => $stdout,
        "stderr" => $stderr,
        "returnCode" => $returnCode,
        "is_resource" => true,
    );
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

	/*if ($_POST['action'] == 'runCommand') {
		$cmd = $_POST['command'];
		echo "$cmd";
		echo shell_exec($cmd);
	}*/

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
			$ignoreDuplicates = $_POST['ignoreDuplicates'] === 'true' ? 'True' : '';
			$cmd = "conda activate overflights && python ../scripts/import_from_editor.py $geojson $trackInfo $import_param_file $ignoreDuplicates 2> $stderrPath";
			// $output = null;

			// $success = exec($cmd, $output);
			// $result = array("success" => $success, "stdout" => $output);
			// echo json_encode($result);
			echo shell_exec($cmd);
		}
	}

	if ($_POST['action'] == 'whoami') {
		echo json_encode(runCmd('conda info --envs'));
		//echo json_encode(runCmd('conda init'));

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

	// import an Excel file directly from scenic-landing-query.html
	if ($_POST['action'] == 'importExcelFile') {
		
		//echo json_encode(runCmd('whoami'));

		$updloadedFile = $_FILES['uploadedFile'];
		$submissionTime = $_POST['submissionTime'];
		// replace special characters in the filename
		$fileName = preg_replace('/[^\w.]+/', '_', basename($_FILES['uploadedFile']['name']));
		$tempFilePath = "temp_files/$fileName";


		// write to temporary file
		if (move_uploaded_file($_FILES['uploadedFile']['tmp_name'], $tempFilePath)) {

			// for some reason, the copied file doesn't inherit since the move to keydb01
			$cmd = "icacls \"$tempFilePath\" /inheritance:e";
			$result = runCmd($cmd);
			if ($result['returnCode'] !== 0) {
			    error_log("icacls failed: " . $result['stderr']);
			}

			// if successful, import the data
			$cmd = "conda run -p ../../overflights python ../scripts/import_excel_landings.py \"$tempFilePath\" \"$submissionTime\" $import_param_file --ignore_warnings";
			$output = $cmd;//null;
			$resultCode = null;

			$result = runCmd($cmd);

			$result["cmd"] = $cmd;
			$result["stdout"] = json_decode($result["stdout"]);

			deleteFile($tempFilePath);

			//$escapedCmd = addslashes($cmd);	
			// for some reason, $output is an array with one element, the stdout string
			//	In order to return just the stdout string (which is a JSON object encoded 
			//	as a string!), I have to get the 0th element, convert to a JSON object,
			//	then convert back to a string to send the respoonse back to the browser
			//$jsonOutput = json_decode($output[0]);
			//echo(json_encode("{\"result\": $jsonOutput, \"resultCode\": $resultCode }"));
			echo(json_encode($result));
		} else {
			echo('{"result": {"errors": "move_uploaded_file() failed"} }');
		}
	} 
}

?>