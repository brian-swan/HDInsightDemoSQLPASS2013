#This script uploads data from a local directory (C:\rawflightdelaydata) to an existing
#container (specifed by $Container) within an existing storage account (specifed by $StorageAccountName)

$StorageAccountName = "your_storage_account_name"
$Container = "your_container_name"
$DataDir = "C:\rawflightdelaydata\"


$StorageAccountKey = Get-AzureStorageKey $StorageAccountName | %{ $_.Primary }
$StorageContext = New-AzureStorageContext –StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey

$files = Get-ChildItem -Path $DataDir
foreach ($file in $files) 
{
	Set-AzureStorageBlobContent `
		-Container $Container `
		-File ($DataDir + $file.name) `
		-Blob $file.name `
		-Context $StorageContext `
		-Force
}