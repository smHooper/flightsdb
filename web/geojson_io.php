

<?php

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
	}//*/
}

?>