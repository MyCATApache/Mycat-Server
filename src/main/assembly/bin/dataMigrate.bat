
REM check JAVA_HOME & java
set "JAVA_CMD=%JAVA_HOME%/bin/java"
if "%JAVA_HOME%" == "" goto noJavaHome
if exist "%JAVA_HOME%\bin\java.exe" goto mainEntry
:noJavaHome
echo ---------------------------------------------------
echo WARN: JAVA_HOME environment variable is not set. 
echo ---------------------------------------------------
set "JAVA_CMD=java"
:mainEntry
REM set HOME_DIR
set "CURR_DIR=%cd%"
cd ..
set "MYCAT_HOME=%cd%"
cd %CURR_DIR%
"%JAVA_CMD%" -server -Xms1G -Xmx1G -XX:MaxPermSize=64M  -XX:+AggressiveOpts -DMYCAT_HOME=%MYCAT_HOME% -cp "..\conf;..\lib\*" io.mycat.util.dataMigrator.DataMigrator -tempFileDir=  -isAwaysUseMaster=true -mysqlBin=  -cmdLength=3*1024 -charset=utf8 -deleteTempFileDir=true -threadCount= -delThreadCount= -queryPageSize=10000

pause