Below is the command to run the script file "exec_script.sh"

sh exec_script.sh /file-path/nasa.tsv

1. script file contains code for running "mvn package" command
2. It then runs spark-submit command with the file path provided. which runs all the spark SQL code to do the required operations

//Running Asterisx DB - Running on port 19002
// http://localhost:19002/query/service
3. Script file contains CURL command to execute the queries on AsterisxDB on port 19002.

Output files created inside project folder:
taskA - contains Average no of bytes per response using SparkSQL

taskB - contains total # count of logs within timestamp --> 804571201 and 804571305
        and the time taken in the operation using SparkSQL.

taskC - contains total # count of logs within timestamp --> 804571201 and 804571305
                and the time taken in the operation using AsteriskDB.

taskD - contains Average no of bytes per response using AsteriskDB.