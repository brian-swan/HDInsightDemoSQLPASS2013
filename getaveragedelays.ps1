#This script runs 3 hive jobs in an HDInsight cluster (for more readable queries, see the hivequeries.hql file).
#The first query creates a table based on a set of .csv files.
#The second query creates a second table by cleansing data from the first table.
#The third query calculates the average weather delay by airport.

#As the script is now, it is configured to run the scripts locally, against the HDInsight developer preview ("OneBox").
#The script assumes that a subset of your data is in HDFS in the /user/data/rawflightdelaydata/ directory.
#You can load data into HDFS using the CopyFromLocal command. The following command (run from the Hadoop command prompt)
#copies all files in C:\rawflightdelaydata\ (locally) to /user/data/rawflightdelaydata/ in HDFS:
#	hadoop fs -copyFromLocal C:\rawflightdelaydata\ /user/data/rawflightdelaydata/
#Output from the script below will be written to the /user/output directory in HDFS.

#The same script can be run against an HDInsight cluster in production. To do so, you will need to comment out the 
#OneBox connection information and uncomment the Production connection information. You will need to update the 
#production connection information with your_cluster_name (assumes you have created a cluster in advance), 
#your_storage_account_name and your_container. As shown, the assumption is that raw data is in a "folder" called
#rawflightdelaydata in your_container. Job output will be written to a "folder" called output.
#When you run the script against a production cluster, you will be prompted for cluster credentials. These are
#the credentials you supplied when creating the cluster.


#$ClusterName = "your_cluster_name";
#$creds = Get-Credential
#$inputdir = "'wasb://your_container@your_storage_account_name.blob.core.windows.net/rawflightdelaydata'"
#$outputdir = "'wasb://your_container@your_storage_account_name.blob.core.windows.net/output'"

#OneBox connection information
$ClusterName = "http://localhost:50111"
$securepwd = ConvertTo-SecureString " " -AsPlainText -Force
$creds = New-Object System.Management.Automation.PSCredential ("hadoop", $securepwd)
$inputdir = "'/user/data/rawflightdelaydata/'"
$outputdir = "'/user/output'"

#Hive query 1
$HiveQueryString1 = "create external table delays_raw (   	FL_DATE string,     	ORIGIN string,     	ORIGIN_CITY string,    	ORIGIN_STATE string,    	DEST string,     	DEST_CITY string,    	DEST_STATE string,     	DEP_DELAY float,     	ARR_DELAY float,     	CARRIER_DELAY float,     	WEATHER_DELAY float,    	NAS_DELAY float,     	SECURITY_DELAY float,    	LATE_AIRCRAFT_DELAY float) 	row format delimited fields 	terminated by ',' 	lines terminated by '\n' 	stored as textfile 	location $inputdir;"
$HiveJobDef = New-AzureHDInsightHiveJobDefinition -Query $HiveQueryString1 -JobName create_delays_raw_table 
Write-Host "Creating external table delays_raw..." -ForegroundColor Green
$HiveJob = Start-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobDefinition $HiveJobDef
while(($State = Get-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobId $HiveJob.JobId | %{$_.State}) -notmatch "Completed|Failed") {Write-Host "$(Get-Date -format 'G'): The job is in [$State] state. Waiting for the job to complete..." -ForegroundColor Cyan; Start-Sleep -s 5;}
Write-Host "Table created!" -ForegroundColor Blue -BackgroundColor Yellow
Write-Host "-------------------" -ForegroundColor Blue -BackgroundColor Yellow

#Hive query 2
$HiveQueryString2 = "create table delays as     select     FL_DATE as flight_date,     substring(ORIGIN, 2, length(ORIGIN) - 2) as origin_airport_code,     substring(ORIGIN_CITY, 2) as origin_city,    substring(ORIGIN_STATE, 2, length(ORIGIN_STATE) - 2)  as origin_state,     substring(DEST, 2, length(DEST) - 2) as dest_airport_code,     substring(DEST_CITY,2) as dest_city,     substring(DEST_STATE, 2, length(DEST_STATE) - 2)  as dest_state,     DEP_DELAY as dep_delay,     ARR_DELAY as arr_delay,     CARRIER_DELAY as carrier_delay,     WEATHER_DELAY as weather_delay,     NAS_DELAY as nas_delay,     SECURITY_DELAY as security_delay,     LATE_AIRCRAFT_DELAY as late_aircraft_delay from delays_raw;"
$HiveJobDef = New-AzureHDInsightHiveJobDefinition -Query $HiveQueryString2 -JobName create_delays_table 
Write-Host "Creating table delays..." -ForegroundColor Green
$HiveJob = Start-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobDefinition $HiveJobDef
while(($State = Get-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobId $HiveJob.JobId | %{$_.State}) -notmatch "Completed|Failed") {Write-Host "$(Get-Date -format 'G'): The job is in [$State] state. Waiting for the job to complete..." -ForegroundColor Cyan; Start-Sleep -s 5;}
Write-Host "Table created!" -ForegroundColor Blue -BackgroundColor Yellow
Write-Host "-------------------" -ForegroundColor Blue -BackgroundColor Yellow

#Hive query 3
$HiveQueryString3 = "INSERT OVERWRITE DIRECTORY $outputdir select origin_airport_code, origin_city, avg(security_delay) from delays group by origin_airport_code, origin_city;"
$HiveJobDef = New-AzureHDInsightHiveJobDefinition -Query $HiveQueryString3 -JobName get_avg_security_delay 
Write-Host "Getting average delays..." -ForegroundColor Green
$HiveJob = Start-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobDefinition $HiveJobDef
while(($State = Get-AzureHDInsightJob -Credential $creds -Cluster $ClusterName -JobId $HiveJob.JobId | %{$_.State}) -notmatch "Completed|Failed") {Write-Host "$(Get-Date -format 'G'): The job is in [$State] state. Waiting for the job to complete..." -ForegroundColor Cyan; Start-Sleep -s 5;}
Write-Host "Average delays computed!" -ForegroundColor Blue -BackgroundColor Yellow
Write-Host "-------------------" -ForegroundColor Blue -BackgroundColor Yellow

