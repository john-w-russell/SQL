@echo off
set userprofile=C:\Users\%username%
set AWS_CONFIG_FILE=C:\Users\%username%\.aws\config

REM Create the settings back file
REM -----------------------------
set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
call tsm settings export --output-config-file "C:\ProgramData\Tableau\Tableau Server\data\tabsvc\files\backups\tableauServerSettingsBackup%CUR_YYYY%%CUR_MM%%CUR_DD%.json"   
IF ERRORLEVEL 1 ( 
echo Error calling tsm settings export, aws sync will not be run return code = %exit_code%
EXIT
)

REM copy the server settings and Tableau backups to S3
REM --------------------------------------------------
call aws s3 sync "C:\ProgramData\Tableau\Tableau Server\data\tabsvc\files\backups\." s3://synthego-tableau/tableau_server_backups/PROD/ --exact-timestamps --exclude "*.lock" 
IF ERRORLEVEL 1 ( 
echo Error calling aws s3 sync, files will not be pruned,  return code = %exit_code%
EXIT
)

REM prune files older than 1 week
REM --------------------------------------------------
forfiles /S /P "C:\ProgramData\Tableau\Tableau Server\data\tabsvc\files\backups" /m *.json /d -7 /c "cmd /c del /q @file"  
forfiles /S /P "C:\ProgramData\Tableau\Tableau Server\data\tabsvc\files\backups" /m *.tsbak /d -7 /c "cmd /c del /q @file"