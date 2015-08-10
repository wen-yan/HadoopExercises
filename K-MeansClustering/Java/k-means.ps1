cls

#### Word Count ###
# create folders...
hadoop fs -rm -r -skipTrash /KMeans

hadoop fs -mkdir /KMeans
hadoop fs -mkdir /KMeans/App
hadoop fs -mkdir /KMeans/Input
#hadoop fs -mkdir /KMeans/MRStatusOutput
# DO NOT create output folder, otherwise no result output !!!
#hadoop fs -mkdir /KMeans/Output

# copy input files
hadoop fs -copyFromLocal "$PSScriptRoot\..\DataSets\*.txt" /KMeans/Input

# run hadoop
$JarPathLocal = "$PSScriptRoot\HadoopJob\out\artifacts\KMeansClustering\KMeansClustering.jar"
hadoop jar $JarPathLocal "-Dkmeans.cluster.count=3" /KMeans/Input/lau15_xy.txt /KMeans/Output

# get result
hadoop fs -ls /KMeans/Output-*
#hadoop fs -cat /KMeans/Input/lau15_xy.txt
#hadoop fs -cat /KMeans/Output-*/ClusterCenter-*
#hadoop fs -cat /KMeans/Output-*/ClusterPoint-*
