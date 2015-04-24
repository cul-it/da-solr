#! /bin/bash
#
# Create master or slave Solr index core(s) on existing multi-core Solr instances.
# Script must be run from within the template core directory. Subdirectories of 
# "./conf" and "./lib" are expected, and the new core will be built using the ".."
# directory.
#
# Frances Webb frances.webb@cornell.edu, April 2014, Cornell University Library


#constants
solrindexDir="/solr/solrindex"

#regex patterns
solrCoreNameP="^[-a-zA-Z0-9\_]+$"
solrServerUrlP="^https?://[-a-zA-Z0-9\.\_]+(:[0-9]+)?/solr/$"
solrServerHostP="^https?://([-a-zA-Z0-9\.\_]+)"
masterCoreUrlP="^https?://[-a-zA-Z0-9\.\_]+(:[0-9]+)?/solr/[-a-zA-Z0-9\_]+/replication$"
solrXmlCoreNameP="name=\"([^\s\"]+)\""

declare -a targetUrls

main()
{
    checkEnvironment
    processArguments "$@"
    createSolrcoreProperties
    for targetUrl in "${targetUrls[@]}"
    do
	createIndexCore $targetUrl
    done
    rm conf/solrcore.properties

}

createSolrcoreProperties()
{
    if [ $coreType = "master" ]; then
	echo "enable.master=true" > ./conf/solrcore.properties
	echo "enable.slave=false" >> ./conf/solrcore.properties
	echo "master.url=${targetUrls[0]}${coreName}/replication" >> ./conf/solrcore.properties
    else
	echo "enable.master=false" > ./conf/solrcore.properties
	echo "enable.slave=true" >> ./conf/solrcore.properties
	echo "master.url=$masterCore" >> ./conf/solrcore.properties
    fi
    if [ ! -w ./conf/solrcore.properties ]; then
	echo Script must have permission to write to ./conf/solrcore.properties
	exit
    fi
}

createIndexCore()
{
    targetUrl=$1
    echo $targetUrl
    if [[ $targetUrl =~ $solrServerHostP ]]; then
	targetHost=${BASH_REMATCH[1]}
	echo $targetHost
	#copy files into place on target Solr
	scp -r "." tomcat@${targetHost}:$solrindexDir/$coreName
	#tell Solr to spin up the new core
	curl "${targetUrl}admin/cores?action=CREATE&name=${coreName}&instanceDir=${coreName}&config=solrconfig.xml&schema=schema.xml&dataDir=data"
	if [ $coreType = "slave" ]; then
	    #activate replication from master
	    curl "${targetUrl}${coreName}/replication?command=fetchindex"
	    #results of command=details can monitor replication
	    # <str name="totalPercent">35.0</str> probably good indicator
	fi
    fi
}


checkEnvironment()
{
    if [[ ( ! -d conf ) || ( ! -d lib) ]]; then
	echo "This script must be executed from within the template core directory."
	echo "Expected subdirectories of the current working directory are 'conf' and 'lib'."
	exit;
    fi
}


checkTargetSystemSolrXmlForSpecificCoreDir()
{
    targetUrl=$1
    if [[ $targetUrl =~ $solrServerHostP ]]; then
	targetHost=${BASH_REMATCH[1]}
	solrXml=$(mktemp)
	scp tomcat@$targetHost:$solrindexDir/solr.xml $solrXml
	coreXml=$(grep -E -e "instanceDir=\"$coreName/?\"" $solrXml)
	if [[ $coreXml =~ $solrXmlCoreNameP ]]; then
	    corePublicName=${BASH_REMATCH[1]}
	else
	    corePublicName=""
	fi
	rm $solrXml
    fi


}



readMasterCoreUrl()
{
    masterCore=$1
    if [[ ! -z $masterCore ]]; then
        re='(.*)XXXX(.*)'
        while [[ $masterCore =~ $re ]]; do
            masterCore=${BASH_REMATCH[1]}${BUILD_NUMBER}${BASH_REMATCH[2]}
        done
        if [[ ! $masterCore =~ $masterCoreUrlP ]]; then
            echo "masterCore url not in correct syntax"
            masterCore=""
        else
            code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $masterCore)
            if [ $code != 200 ]; then
                echo "master core URL $masterCore doesn't appear to ba a valid core."
                masterCore=""
            fi
        fi
    fi;
}

readCoreName()
{
    coreName=$1
    if [[ ! -z $coreName ]]; then
        re='(.*)XXXX(.*)'
        while [[ $coreName =~ $re ]]; do
            coreName=${BASH_REMATCH[1]}${BUILD_NUMBER}${BASH_REMATCH[2]}
        done
        if [[ ! $coreName =~ $solrCoreNameP ]]; then
            echo "CoreName is not in correct syntax. Though Solr doesn't impose this restriction so tightly, we are currently validating core names to alphanumeric characters, dashes, and underscores."
            coreName=""
        fi
    fi;
}

readHostSolrUrl()
{
    solrUrl="$1"
    if [[ ! -z $solrUrl ]]; then
	if [[ ! $solrUrl =~ $solrServerUrlP ]]; then
	    echo "solrUrl url $solrUrl not in correct syntax"
	    echo "expected syntax is http[s]://HOSTNAME[:PORT]/solr/"
	    solrUrl=""
	else
	    code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $solrUrl)
	    if [ $code != 200 ]; then
            echo "The solr server at $solrUrl appears to be unavailable"
            solrUrl=""
	    else
            code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' "${solrUrl}/$coreName")
            if [ $code == 200 ]; then
                echo "The solr server at $solrUrl appears to already have a core called $coreName"
                solrUrl=""
            else
                checkTargetSystemSolrXmlForSpecificCoreDir "$solrUrl"
                if [ "$corePublicName" != "" ]; then
                    echo "The solr server at $solrUrl appears to already have a core in a directory called $coreName, but with the public name $corePublicName."
                    solrUrl=""
                fi
            fi
	    fi
	fi
    fi
    if [[ ! -z $solrUrl ]]; then
	echo "Target Solr server: $solrUrl"
	targetUrls[${#targetUrls[@]}]=$solrUrl
    fi
    
}

processArguments()
{
    coreType="$1"
    if [ ! $coreType ]; then
	echo "First argument must be \"master\" or \"slave\" to determine type of core."
	exit;
    elif [ $coreType = "master" ]; then
        #need core name, server location
	echo "master";
	readCoreName "$2"
	if [ ! $coreName ]; then
	    echo "Second argument for creating a master core instance must be a valid Solr core name for the new core."
	    exit;
	else
	    echo "New master core name: $coreName"
	fi
	readHostSolrUrl "$3"
    elif [ $coreType = "slave" ]; then
        #need master core url, core name, server location(s)
	echo "Slave core instance."
	readMasterCoreUrl "$2"
	if [ ! "$masterCore" ]; then
	    echo "Second argument for creating a slave core instance must be a valid URL for the master core."
	    echo "The syntax of the URL should be <http[s]://MASTERHOSTNAME[:PORT]/solr/CORE/replication>."
	    exit;
	else
	    echo "New slave core(s) to replicate from: $masterCore"
	fi
	readCoreName "$3"
	if [ ! $coreName ]; then
	    echo "Third argument for creating slave core instance(s) must be a valid Solr core name for the new core(s)."
	    exit;
	else
	    echo "New slave core name: $coreName"
	fi
	shift 3
	for url in $@; do
	    readHostSolrUrl $url
	done
	if [ ${#targetUrls[@]} -eq 0 ]; then
	    echo "Final argument(s) when creating new slave core instance(s) must be valid URL(s) for the instance(s)."
	    echo "The syntax of the URL should be <http[s]://SLAVEHOSTNAME[:PORT]/solr/>."
	    exit
	fi
	echo "targetUrls: ${#targetUrls[@]}"
    else
	echo "First argument must be \"master\" or \"slave\" to determine type of core.\n"
	exit;
    fi
}

main "$@"
#coreDir = `mkdtemp -d`






#rm -rf coreDir

