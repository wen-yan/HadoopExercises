cls

$Root = 'C:\Work\GitHub\wensagi\HadoopExercises\K-MeansClustering\Java'
$Root = $PSScriptRoot

#### k-means clustering ###
$ClusterName = "http://localhost:50111" # emulator cluster name is always http://localhost:50111
$ContainerName = "hdpcls"
$JarFileName = "KMeansClustering.jar"

$StorageContext = New-AzureStorageContext -Local

# create container...
New-AzureStorageContainer -Context $StorageContext -Name $ContainerName -Permission Blob


# create folders...
#Remove-AzureStorageBlob -Context $StorageContext -Container $ContainerName -Blob ""

$DataSetsFolder = "$Root\..\DataSets"
foreach($LocalFile in Get-ChildItem -Path "$DataSetsFolder\*.txt")
{
    $FileName = $LocalFile.Name
    $BlobName = "KMeans/Input/$FileName"

    Set-AzureStorageBlobContent -Context $StorageContext -Container $ContainerName -File $LocalFile.FullName -Blob $BlobName -Force
}

# Copy jar to storage...
$JarPathLocal = "$Root\HadoopJob\out\artifacts\KMeansClustering\$JarFileName"
$JarBlob = "KMeans/App/$JarFileName"
Set-AzureStorageBlobContent -Context $StorageContext -Container $ContainerName -File $JarPathLocal -Blob $JarBlob -Force

# Delete outputs..
Get-AzureStorageBlob -Context $StorageContext -Container $ContainerName -Blob "KMeans/Output/*" |Remove-AzureStorageBlob
Remove-AzureStorageBlob -Context $StorageContext -Container $ContainerName -Blob "KMeans/Output"

Get-AzureStorageBlob -Context $StorageContext -Container $ContainerName -Blob "KMeans/*"

# Run hadoop...
$JobDefinition = New-AzureHDInsightMapReduceJobDefinition `
    -JarFile "wasb://hdpcls@storageemulator/$JarBlob" `
    -ClassName "kmeansclustering.KMeansClusteringJob" `
    -Defines @{ "kmeans.cluster.count"="3" } `
    -Arguments "wasb://hdpcls@storageemulator/KMeans/Input/lau15_xy.txt", "wasb://hdpcls@storageemulator/KMeans/Output"


$Creds = Get-Credential -UserName "hadoop" -Message "Enter password"
$Job = Start-AzureHDInsightJob -Cluster $ClusterName -JobDefinition $JobDefinition -Credential $Creds -Verbose
Wait-AzureHDInsightJob -Credential $Creds -Job $Job -WaitTimeoutInSeconds 3600

#Select-AzureSubscription -Current
#Get-AzureHDInsightJobOutput -Cluster $ClusterName -JobId $Job.JobId -StandardError -Verbose

# Show result...
Get-AzureStorageBlob -Context $StorageContext -Container $ContainerName -Blob "KMeans/Output/*"

