@echo off
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

cd /d "%SCRIPT_DIR%"
%ANACONDAPATH%\python -c "from running_main import rr_running_main; rr_running_main()"
%ANACONDAPATH%\python -c "from running_main import xy_future_running_main; xy_future_running_main()"
%ANACONDAPATH%\python -c "from running_main import renr_future_running_main; renr_future_running_main()"
pause