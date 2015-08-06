cls

#### Word Count ###
# create folders...
hadoop fs -rm -r -skipTrash /WordCount
#hadoop fs -rm /WordCount/Apps
#hadoop fs -rm /WordCount/Input
#hadoop fs -rm /WordCount/Output
#hadoop fs -rm /WordCount/MRStatusOutput

hadoop fs -mkdir /WordCount
hadoop fs -mkdir /WordCount/Apps
hadoop fs -mkdir /WordCount/Input
#hadoop fs -mkdir /WordCount/MRStatusOutput
# DO NOT create output folder, otherwise no result output !!!
#hadoop fs -mkdir /WordCount/Output

# copy input files
Out-File -InputObject "this is a test, THIS IS A TEST." -FilePath "$PSScriptRoot\src\input\input.txt" -Encoding ascii
Out-File -InputObject "this is the second test, THIS IS THE SECOND TEST." -FilePath "$PSScriptRoot\src\input\input1.txt" -Encoding ascii

hadoop fs -copyFromLocal "$PSScriptRoot\src\input\*.txt" /WordCount/Input

# create stop word file...
Out-File -InputObject ".`n," -FilePath "$PSScriptRoot\src\input\stopword.wrd" -Encoding ascii
hadoop fs -copyFromLocal "$PSScriptRoot\src\input\stopword.wrd" /WordCount/Input

hadoop fs -ls /WordCount/Input


# run hadoop
$JarPathLocal = "$PSScriptRoot\src\out\artifacts\WordCount_jar\WordCount.jar"
hadoop jar $JarPathLocal "-Dwordcount.case.sensitive=false" /WordCount/Input/*.txt /WordCount/Output -skip /WordCount/Input/stopword.wrd

# get result
hadoop fs -ls /WordCount/Output
hadoop fs -cat /WordCount/Output/part-r-*
