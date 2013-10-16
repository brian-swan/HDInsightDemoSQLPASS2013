The scripts in this repository were part of a Windows Azure HDInsight demo
I did at SQL PASS 2013. I'm sharing them as is for educational purposes at
the request of session attendees.

To best understand how these PowerShell scripts were used, you should 
understand the demo prerequisites and the demo flow:

Prerequisites:
1. You have a Windows Azure account.
2. You have installed the HDInsight Developer Preview ("OneBox") via the Web Platform Installer: http://www.microsoft.com/web/downloads/platform.aspx
3. You have installed the Windows Azure PowerShell cmdlets and configured them for your account: http://www.windowsazure.com/en-us/manage/install-and-configure-windows-powershell/
4. You have installed the HDInsight PowerShell cmdlets: http://go.microsoft.com/fwlink/?LinkID=325563&clcid=0x409
5. Create a Windows Azure Storage account and a container in the account: http://www.windowsazure.com/en-us/manage/services/storage/how-to-create-a-storage-account/
6. Create a Windows Azure SQL database: http://www.windowsazure.com/en-us/manage/services/sql-databases/getting-started-w-sql-databases/

Demo flow:
1. Download flight delay data from RITA: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time.
The fields selected were these:
	 "FL_DATE",
	"ORIGIN",
	"ORIGIN_CITY_NAME",
	"DEST",
	"DEST_CITY_NAME",
	"DEP_DELAY_NEW",
	"ARR_DELAY_NEW",
	"CARRIER_DELAY",
	"WEATHER_DELAY",
	"NAS_DELAY",
	"SECURITY_DELAY",
	"LATE_AIRCRAFT_DELAY"

2. Upload data to your container within your storage account - use the uploaddata.ps1 script.
3. Upload a subset of the data to HDFS in OneBox. The following command (run from the Hadoop command prompt)
copies all files in C:\rawflightdelaydata\ (locally) to /user/data/rawflightdelaydata/ in HDFS:
	
	hadoop fs -copyFromLocal C:\rawflightdelaydata\ /user/data/rawflightdelaydata/
	
4. Run Hive queries locally - use the getaveragedelays.ps1 script.
5. Create a cluster in HDInsight - use the createnewcluster.ps1 script.
6. Change cluster connection information in getaveragedelays.ps1 and run it against your HDInsight cluster.
7. RDP into the cluster, open the sqoop cmd promt and run the sqoop query in sqoopquery.txt.
8. Query your SQL Database for results.