folder=$1
location="/home/rajeebp/"$folder"/src/main/scala/"

if [ $# == 2 ]; then 
	file_path=$2
	cat $file_path | while read line
	do
	entry=$line
	table=$(echo "$entry" | sed 's/.scala//g' | sed 's/o_//g' | awk '{print tolower($0)}')  
	newF="/home/rajeebp/master_jars/"$folder"/"$table
	if [ -d "$newF" ]; then
	  echo  "${newF}:Exists"
	else
	   mkdir ${newF}
	   src_location="/home/rajeebp/"$folder"/*"
	   des_location=$newF"/"
	    cp  -r $src_location $des_location
	   target_dir=$des_location"target"
	   source_dir=$des_location"src/main/scala"
	   sbt_file=$des_location"build.sbt"
	   rm -rf $target_dir
	   rm -rf $source_dir
	   rm -f $sbt_file
	   echo "name := \"$table\"  " > $sbt_file
	   echo "  version := \"1.0\"  "   >> $sbt_file
	   echo "  scalaVersion := \"2.11.8\"  "   >> $sbt_file
	   echo "  autoScalaLibrary := false "   >> $sbt_file
	   echo "scalaHome := Some(file(\"/home/rajeebp/scala/\"))"   >> $sbt_file
	   mkdir $source_dir
	   sourceloc=$location$entry
	    cp  -r  $sourceloc $source_dir
	    cp  -r /home/rajeebp/iqp_fact/src/main/scala/Utils.scala $source_dir
	    cp  -r /home/rajeebp/iqp_fact/src/main/scala/PlrFetch.scala $source_dir
	   cd ${newF}
	   ~/sbt/bin/sbt compile
	   ~/sbt/bin/sbt package

	fi
	done 

else
	for entry in "$location"/*
	do
	table=$(basename "$entry" | sed 's/.scala//g' | sed 's/o_//g' | awk '{print tolower($0)}')  
	newF="/home/rajeebp/master_jars/"$folder"/"$table
	if [ -d "$newF" ]; then
	  echo  "${newF}:Exists"
	else
	   mkdir ${newF}
	   src_location="/home/rajeebp/"$folder"/*"
	   des_location=$newF"/"
	   cp  -r $src_location $des_location
	   target_dir=$des_location"target"
	   source_dir=$des_location"src/main/scala"
	   sbt_file=$des_location"build.sbt"
	   rm -rf $target_dir
	   rm -rf $source_dir
	   rm -f $sbt_file
	   echo "name := \"$table\"  " > $sbt_file
	   echo "  version := \"1.0\"  "   >> $sbt_file
	   echo "  scalaVersion := \"2.11.8\"  "   >> $sbt_file
	   echo "  autoScalaLibrary := false "   >> $sbt_file
	   echo "scalaHome := Some(file(\"/home/rajeebp/scala/\"))"   >> $sbt_file
	   mkdir $source_dir
	   cp  -r $entry $source_dir
	   cp  -r /home/rajeebp/iqp_fact/src/main/scala/Utils.scala $source_dir
	   cp  -r /home/rajeebp/iqp_fact/src/main/scala/PlrFetch.scala $source_dir
	   cd  ${newF}
	   ~/sbt/bin/sbt compile
	   ~/sbt/bin/sbt package

	fi
	done 

fi