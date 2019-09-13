rem Convenience script to copy the latest version of overflights scripts whenever pushing changes to GitHub

git push
xcopy ""%~dp0scripts\*"" \\inpdenards\overflights\scripts /e /h /y