

<?php
//echo "Script called"
if (isset($_POST["action"])) {
	
	if ($_POST["action"] == "getFiles") {
		$json_files = array_filter(glob('*geojsons.json'));
		echo json_encode($json_files);
	}
}
?>