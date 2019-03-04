#!/bin/bash

################################################################################
# SCRIPT_NAME     : ty_dsv_to_datax_zl.sh
#
# CREATE_TIME     : 2018/04/10
# AUTHOR          : Mochou_liqb
#
# DESCRIBETION    : get zl_dsv_json
# PARAMETER       : 1 baseConf, such as HX
# EXAMPLE         : ./ty_dsv_to_datax_zl.sh HX
# 
# UPDATE_RECORD   : change comments from Chinese to English and kill some bugs
#
# DATE      OPERATION       CZR         DESCRIBETION              		
# ________  _____________   ________    __________________________________
#
# 2018/05/26  UPDATE Mochou_liqb  
################################################################################

if [ $# != 1 ] ; then 
echo "params error"
exit 1;
fi
##对dsv生成json后,将其后缀改为jsv
##系统名称，如 HX
####T_TY_HX_DJ_NSRXX
baseConf=$1

if [ $# == 1 ] ; then 
echo "START..."
fi 
##SYSCODE_NAME such as HX
baseConf="${1}"
##default "Q"
xtype="Q"
#xtype="R_Z"  or  xtype="Q"
###############################################################################
#################################PUBLIC VARIABLES##############################
##create import dirs
##json base dirs
versionBase="/home/admin/version/TY"
jsonBase="${versionBase}/${baseConf}/json"
##sql base dirs
sqlBase="${versionBase}/sql"
##conf base dirs
confBase="${versionBase}/${baseConf}/conf"
confFile="${confBase}/ty_createJson_ql.conf"
##shell base dirs
shellBase="${versionBase}/shell"
##get table's columns without lob_columns
splitF="#"
##source file
source ${confBase}/ty_datasource.conf
##version
curdt="`date +%Y%m%d%H%M%S`"
reader="txtfilereader"

################################################################################
##############oracle sqlplus  environment variable by system servers############
if [[ "${reader}" == "oraclereader" ]];then 
export ORACLE_HOME=${TY_ORACLE_HOME}
export LD_LIBRARY_PATH=${TY_LD_LIBRARY_PATH}
export NLS_LANG="SIMPLIFIED CHINESE_CHINA.AL32UTF8"
export PATH=${ORACLE_HOME}/bin:${LD_LIBRARY_PATH}:${PATH}
fi


Tm="${jsonBase}/DSV"
##目录检测
if [ ! -d ${Tm} ] ;then
	mkdir -p ${Tm}
fi
if [ ! -d ${sqlBase} ] ;then
	mkdir -p ${sqlBase}
fi
if [ ! -d ${confBase} ] ;then
	mkdir -p ${confBase}
fi




##json文件进行版本记录
##resData:[=0不创建json | =1创建json]
################################################################################
function LogJsonPropertes(){
jsonProperties=${3}
echo "${jsonProperties}" >>${jsonBase}/dsv_version
}

################################################################################
##json生成
function createJsonFile(){
	renameCsv=${1}
	encoding=${2}
	columns=${3}
	splitFlag=${4}
	sselect=${5}
	odpsTable=${6}
	stable=${7}
	jsonName=${8}
	jsonProperties=${9}
	echo "${jsonName}文件创建中"
	flag=$?
	echo "执行返回值:$flag"
	json="{
	\"job\": {
	  \"content\":[
		{
		  \"reader\":{
			\"name\":\"ftpreader\",
			\"parameter\":{
				\"protocol\": \"sftp\",
				\"host\": \"\${ftpIp}\",
				\"port\": \${ftpPort},
				\"username\": \"\${ftpUser}\",
				\"password\": \"\${ftpPasswd}\",
				\"path\": [\"${renameCsv}\"],
				\"fileFormat\": \"csv\",
				\"encoding\": \"utf-8\",
				\"column\": [$columns],
				 \"fieldDelimiter\": \"${splitFlag}\"
			}
		  },
		   \"writer\":{
                \"name\":\"odpswriter\",
                \"parameter\":{
                \"accessId\":\"\${odpsaccessId}\",
                \"accessKey\":\"\${odpsaccessKey}\",
                \"accountType\":\"aliyun\",
                \"column\":[${sselect}],
                \"odpsServer\":\"${odpsServer}\",
                \"partition\":\"rfq=\$bizdate\",
                \"project\":\"\${project}\",
                \"table\":\"${odpsTable}\",
                \"truncate\": \"true\",
		\"emptyAsNull\":\"true\"
                }
            }
		}
	  ],
	  \"setting\":{
		\"errorLimit\":{
		\"record\":0
		 },
		\"speed\":{
		  \"channel\":\"2\"
		}
	  }
	 }
	}"
	Tm="${jsonBase}/DSV/"
	echo -e "${json}">${Tm}/${jsonName}
}

##
function formatToFile(){
ftpPath=$1
tableName=$2
odpsTable=$3
owner=$4
##查询owner.tablename对应的列名、对应到odps的数据类型
tableInfoSQL="
SELECT b.column_name || ':' || b.odpstype
  FROM all_col_comments a
 inner JOIN (SELECT owner,
                    table_name,
                    column_name,
                    CASE
                      WHEN A.DATA_TYPE IN ('CHAR', 'VARCHAR2', 'VARCHAR') OR
                           A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND
                           A.data_precision >= 19 THEN
                       'string'
                      WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND
                           A.data_precision < 19 OR
                           A.DATA_TYPE = 'NUMBER' AND A.DATA_LENGTH = 22 AND
                           A.DATA_PRECISION IS NULL AND A.DATA_SCALE = 0 THEN
                       'bigint'
                      WHEN A.DATA_TYPE IN
                           ('BINARY_FLOAT', 'BINARY_DOUBLE', 'FLOAT') THEN
                       'double'
                      WHEN A.DATA_TYPE IN ('DATE', 'TIMESTAMP(6)') THEN
                       'string'
                      WHEN A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE > 0 AND
                           A.data_precision - A.DATA_SCALE <= 36 AND
                           A.DATA_SCALE <= 18 THEN
                       'decimal'
                      WHEN A.data_type = 'BOOLEAN' THEN
                       'boolean'
					   WHEN A.data_type = 'LONG' THEN
                       'long'
					   WHEN A.data_type = 'NCLOB' THEN
                       'nclob'
					   WHEN A.data_type = 'CLOB' THEN
                       'clob'
					    WHEN A.data_type = 'BLOB' THEN
                       'blob'
                      ELSE
                       'string'
                    END AS odpstype,
                    data_length
               FROM all_tab_columns A
              WHERE owner = upper('${owner}')
                AND table_name = upper('${tableName}')
				) b ON a.column_name =
                                                                             b.column_name 
 INNER JOIN all_tab_cols c ON c.COLUMN_NAME = a.column_name
                          and c.OWNER = upper('${owner}')  
                          and c.table_name= upper('${tableName}')
 WHERE a.owner = upper('${owner}')
   AND a.TABLE_NAME = upper('${tableName}')
 order by C.COLUMN_ID;
"

##查询该表的所有字段，按column_id进行排序，查询出来后进行一一匹配dsv文本所在列，对大字段进行跳过处理"
result=`sqlplus -S ${user}/${pass}@${jdbc} <<END
set heading off
set feedback off
set pagesize 0
set verify off
set echo off
${tableInfoSQL}
quit;
END`

for str in `echo $result`
do
echo "数据库查询结果#:[$str]"
done

echo "####程序查询数据库列名......"
#echo "执行结果：$result"
OLD_IFS2="$IFS2"
IFS2=","
arr2=($result)
IFS2="$OLD_IFS2"
tm=""
sselect=""
##内外循环，匹配出表字段在dsv的位置
echo -e "字段匹配循环等待中......."
##ogg增量文件的字段开始
firstStart=2
dtIndex=$firstStart
for i in ${arr2[@]}; do
##用于跳过列名
let dtIndex++
let dtIndex++
tmDtIndex=`expr $dtIndex - 1`
fieldNameList=`echo $i|awk -F ':' '{print $1}' |tr [a-z] [A-Z]`
fieldTypeList=`echo $i|awk -F ':' '{print $2}'`
if [[ "$fieldTypeList" != "long" ]] && [[ "$fieldTypeList" != "nclob" ]] && [[ "$fieldTypeList" != "clob" ]] && [[ "$fieldTypeList" != "blob" ]] ;then 
tm="$tm""{\"index\":$tmDtIndex,\"type\":\"string\"},"
sselect="$sselect""\"$fieldNameList\","
else
echo "！！跳过大字段类型..."
##用于跳过列名
#let dtIndex++
#let dtIndex++
fi
done
##追加数据操作类型
tm="$tm""{\"index\":0,\"type\":\"string\"},"
sselect="$sselect""\"ypt_ysjczlx\","
##追加数据操作时间
tm="$tm""{\"index\":1,\"type\":\"string\"},"
sselect="$sselect""\"ypt_ysjczxl\","
tm=`echo $tm|sed 's/,$//'`
sselect=`echo $sselect|sed 's/,$//'`
sselect=${sselect//,\"RANGE\"/,\"FW\"}
#echo "! csv列信息：【$tm】"
#echo "! odps列信息：【$sselect】"
#jsonName="T_TY_${baseConf}_${tableName}_DSV_${xtype}.json"
jsonName="${odpsTable}_DSV_${xtype}.json"
jsonTable="${owner}"".""${tableName}"
createJsonFile "$ftpPath" "$encoding" "$tm" "$spiltFlag" "$sselect" "$odpsTable" "$jsonTable" "$jsonName"
echo "创建JSON:[${jsonTable}  &&  ${jsonName}]成功"
}


##datasource中库信息是来源配置
##json需要指向ftp上的文件路径
function readDsv(){
##ftp中的文件目录
echo ""
echo ""
##今天的时间
#dataTime="`date +%Y%m%d`"
##昨天的时间
dataTime="`date  +"%Y%m%d" -d  "-1 days"`"
##遍历增量配置文件
for line in `cat ${confBase}/ty_createJson_zl.conf | grep -v "^#"`; do
##表所属域名
local owner=`echo $line | awk -F '|' '{print $1}' | tr [a-z] [A-Z]`
##表名
local tableName=`echo $line | awk -F '|' '{print $2}' | tr [a-z] [A-Z]`
##odps表名
local odpsTable=`echo $line | awk -F '|' '{print $3}' | tr [a-z] [A-Z]`
filterStr="${owner}.${tableName}.2*.dsv"
##ftp上对应表的路径，根据ogg和文档服务器实际去修改
#ftpPath="/""$dataTime""/""$tableName""/""${filterStr}"
ftpPath="${ftpBasePath}/\$bizdate/${filterStr}"
echo "ftp对应的路径为：$ftpPath"
formatToFile "$ftpPath" "$tableName" "$odpsTable" "$owner"
done
}

function checkDBlink(){
SQL="select to_char(sysdate,'yyyy-mm-dd') today from dual;"
ii=0
flag=false
DATE=$(date +%Y-%m-%d)
while [ ${ii} -lt 3 ]
do
OK=`sqlplus -S ${user}/${pass}@${jdbc} <<END
set heading off
set feedback off
set pagesize 0
set verify off
set echo off
$SQL
quit;
END`
ii=$[ii+1]
if [[ ${OK} == ${DATE} ]] ; then 
flag=true;
echo "数据库连接连接成功，开始执行脚本!";
break; 
fi
sleep 5;
done
if [[ ${flag} == false ]] ; then echo "数据库连接失败，请检查数据库连接信息!"; 
else 
readDsv
fi
exit 0
}

################################################################################
if [[ "${reader}" == "txtfilereader" ]];then 
	checkDBlink
else 
	echo ""
	echo "####!!![数据源请配置为txtfilereader，否则文件无法初始化]" 
fi 
