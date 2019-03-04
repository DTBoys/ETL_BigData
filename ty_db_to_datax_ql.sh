#!/bin/bash
################################################################################
# SCRIPT_NAME     : ty_db_to_datax_ql.sh
#
# CREATE_TIME     : 2018/04/10
# AUTHOR          : Mochou_liqb
#
# DESCRIBETION    : make some json files to use by dataX--QL
# PARAMETER       : 1 baseConf, such as HX
# EXAMPLE         : ./ty_db_to_datax_ql.sh HX
#
# UPDATE_RECORD   : change comments from Chinese to English and kill some bugs
#
# DATE      OPERATION       CZR         DESCRIBETION
# ________  _____________   ________    __________________________________
#
# 2018/05/26  UPDATE Mochou_liqb
################################################################################

if [ $# == 1 ] ; then
	echo "START..."
fi
##SYSCODE_NAME such as HX
baseConf="$1"
##default "Q"
xtype="Q"
#xtype="R_Z"  or  xtype="Q"
###############################################################################
#################################PUBLIC VARIABLES##############################
##create import dirs
##json base dirs
versionBase="/home/admin/version/TY"
jsonBase="$versionBase/$baseConf/json"
##sql base dirs
sqlBase="$versionBase/sql"
##conf base dirs
confBase="$versionBase/$baseConf/conf"
confFile="$confBase/ty_createJson_ql.conf"
##shell base dirs
shellBase="$versionBase/shell"
##get table's columns without lob_columns
splitF="#"
##source file
source $confBase/ty_datasource.conf
##version
curdt="`date +%Y%m%d%H%M%S`"
#################################PUBLIC VARIABLES##############################
###############################################################################
## configure datax's channelNums
channel_sm=1
channel_big=10
channel_fq=8
################################################################################
##############oracle sqlplus  environment variable by system servers############
if [[ "$reader" == "oraclereader" ]];then
	export ORACLE_HOME=$TY_ORACLE_HOME
	export LD_LIBRARY_PATH=$TY_LD_LIBRARY_PATH
	export NLS_LANG="SIMPLIFIED CHINESE_CHINA.AL32UTF8"
	export PATH=$ORACLE_HOME/bin:$LD_LIBRARY_PATH:$PATH
fi

Tm="$jsonBase/$xtype"
##check dirs
if [ ! -d $Tm ] ;then
	mkdir -p $Tm
fi
if [ ! -d $sqlBase ] ;then
	mkdir -p $sqlBase
fi
if [ ! -d $confBase ] ;then
	mkdir -p $confBase
fi

################################################################################
##ZL json making
function createZlJsonFile(){
	stable=$1
	local sourceSelect="$2,\"'S'\",\"'00000'\""
	local targetSelect="$3,\"ypt_ysjczlx\",\"ypt_ysjczxl\""
	local odpsTable=$4
	local jsonName=$5
	local swhere=$6
	local splitPk=$7
	local JsonSecondPath=$8
	local channelNum=$9
	local partitionT=$10
	if [ "$partitionT" != "" ] ; then
		stable=`echo $stable | awk -F '.' '{print $1"."$2" "$3}'`
	fi
	truncate="true"
	if [ "$JsonSecondPath" == "PAR" ] ; then
		truncate="false"
	fi
	swhere=`echo $swhere|awk -F '@' '{print $2}'`
	splitPk=`echo $splitPk|awk -F ':' '{print $2}'`
	TSm="$jsonBase/$xtype/$JsonSecondPath"

	if [ ! -d $TSm ] ;then
		mkdir -p $TSm
	fi

	if [ "$partitionT" != "" ] ; then
		rfqName=`echo $partitionT | awk -F '_PARTITION_' '{print $2}'`
		json="{
			\"job\": {
				\"content\":[
				{
					\"reader\":{
						\"name\":\"$reader\",
						\"parameter\":{
							\"column\":[$sourceSelect],
							\"splitPk\": \"$splitPk\",
							\"connection\":[
							{
								\"jdbcUrl\":[\"\$odpsJdbc\"],
								\"table\":[\"$stable\"]
							}
							],
							\"fetchSize\":1024,
							\"password\":\"\$pass\",
							\"username\":\"\$user\",
							\"where\":\"$swhere\"
						}
					},
					\"writer\":{
						\"name\":\"odpswriter\",
						\"parameter\":{
							\"accessId\":\"\$odpsaccessId\",
							\"accessKey\":\"\$odpsaccessKey\",
							\"accountType\":\"aliyun\",
							\"column\":[$targetSelect],
							\"odpsServer\":\"$odpsServer\",
							\"partition\":\"rfq=$rfqName\",
							\"project\":\"\$project\",
							\"table\":\"$odpsTable\",
							\"truncate\": \"true\"
						}
					}
				}
				],
				\"setting\":{
					\"errorLimit\":{
						\"record\":0
					},
					\"speed\":{
						\"channel\":\"$channelNum\"
					}
				}
			}
		}"
	else
		json="{
			\"job\": {
				\"content\":[
				{
					\"reader\":{
						\"name\":\"$reader\",
						\"parameter\":{
							\"column\":[$sourceSelect],
							\"splitPk\": \"$splitPk\",
							\"connection\":[
							{
								\"jdbcUrl\":[\"\$odpsJdbc\"],
								\"table\":[\"$stable\"]
							}
							],
							\"fetchSize\":1024,
							\"password\":\"\$pass\",
							\"username\":\"\$user\",
							\"where\":\"$swhere\"
						}
					},
					\"writer\":{
						\"name\":\"odpswriter\",
						\"parameter\":{
							\"accessId\":\"\$odpsaccessId\",
							\"accessKey\":\"\$odpsaccessKey\",
							\"accountType\":\"aliyun\",
							\"column\":[$targetSelect],
							\"odpsServer\":\"$odpsServer\",
							\"partition\":\"rfq=\$bizdate\",
							\"project\":\"\$project\",
							\"table\":\"$odpsTable\",
							\"truncate\": \"true\"
						}
					}
				}
				],
				\"setting\":{
					\"errorLimit\":{
						\"record\":0
					},
					\"speed\":{
						\"channel\":\"$channelNum\"
					}
				}
			}
		}"
	fi


	echo -e "$json">$TSm/$jsonName.json
}

################################################################################
##QL json making
function createQlJsonFile(){
	local stable=$1
	local sourceSelect="$2,\"'S'\",\"'00000'\""
	local targetSelect="$3,\"ypt_ysjczlx\",\"ypt_ysjczxl\""
	local odpsTable=$4
	local jsonName=$5
	local swhere=$6
	local splitPk=$7
	local JsonSecondPath=$8
	local channelNum=$9
	local partitionT=$10

	if [ "$partitionT" != "" ] ; then
		stable=`echo $stable | awk -F '.' '{print $1"."$2" "$3}'`
	fi

	truncate="true"
	if [ "$JsonSecondPath" == "PAR" ] ; then
		truncate="false"
	fi

	swhere=`echo $swhere|awk -F '@' '{print $2}'`
	splitPk=`echo $splitPk|awk -F ':' '{print $2}'`
	TSm="$jsonBase/$xtype/$JsonSecondPath"
	##目录检测
	if [ ! -d $TSm ] ;then
		mkdir -p $TSm
	fi

	if [ "$partitionT" != "" ] ; then
		rfqName=`echo $partitionT | awk -F '_PARTITION_' '{print $2}'`
		json="{
			\"job\": {
				\"content\":[
				{
					\"reader\":{
						\"name\":\"$reader\",
						\"parameter\":{
							\"column\":[$sourceSelect],
							\"splitPk\": \"$splitPk\",
							\"connection\":[
							{
								\"jdbcUrl\":[\"\$odpsJdbc\"],
								\"table\":[\"$stable\"]
							}
							],
							\"fetchSize\":1024,
							\"password\":\"\$pass\",
							\"username\":\"\$user\",
							\"mandatoryEncoding\":\"UTF-8\"
						}
					},
					\"writer\":{
						\"name\":\"odpswriter\",
						\"parameter\":{
							\"accessId\":\"\$odpsaccessId\",
							\"accessKey\":\"\$odpsaccessKey\",
							\"accountType\":\"aliyun\",
							\"column\":[$targetSelect],
							\"odpsServer\":\"$odpsServer\",
							\"partition\":\"rfq=$rfqName\",
							\"project\":\"\$project\",
							\"table\":\"$odpsTable\",
							\"truncate\": \"$truncate\"
						}
					}
				}
				],
				\"setting\":{
					\"errorLimit\":{
						\"record\":0
					},
					\"speed\":{
						\"channel\":\"$channelNum\"
					}
				}
			}
		}"
	else
		json="{
			\"job\": {
				\"content\":[
				{
					\"reader\":{
						\"name\":\"$reader\",
						\"parameter\":{
							\"column\":[$sourceSelect],
							\"splitPk\": \"$splitPk\",
							\"connection\":[
							{
								\"jdbcUrl\":[\"\$odpsJdbc\"],
								\"table\":[\"$stable\"]
							}
							],
							\"fetchSize\":1024,
							\"password\":\"\$pass\",
							\"username\":\"\$user\",
							\"mandatoryEncoding\":\"UTF-8\"
						}
					},
					\"writer\":{
						\"name\":\"odpswriter\",
						\"parameter\":{
							\"accessId\":\"\$odpsaccessId\",
							\"accessKey\":\"\$odpsaccessKey\",
							\"accountType\":\"aliyun\",
							\"column\":[$targetSelect],
							\"odpsServer\":\"$odpsServer\",
							\"partition\":\"rfq=\$bizdate\",
							\"project\":\"\$project\",
							\"table\":\"$odpsTable\",
							\"truncate\": \"$truncate\"
						}
					}
				}
				],
				\"setting\":{
					\"errorLimit\":{
						\"record\":0
					},
					\"speed\":{
						\"channel\":\"$channelNum\"
					}
				}
			}
		}"
	fi
	echo -e "$json">$TSm/$jsonName.json
}


#######HX_CS_QG|CS_DJ_SSXDGJDQGLB|T_TY_HX_CS_DJ_SSXDGJDQGLB|SSXD_DM|NO|159
#######是否为分区表、记录数大小：小表、分区表、非分区大表
#######小表：101
#######分区表：102
#######非分区大表（500~1000）：103
#######非分区大表（>1000）：104
function calculateTab(){
	isParTab=$1
	tabCount=$2
	resNum=0
	if [ "$isParTab" = "YES" ] ; then
		##分区表
		resNum=102
	else
		if [ $tabCount -ge 5000000 ] && [ $tabCount -lt 10000000 ] ; then
			##区间：500w ~ 1000w
			resNum=103
		elif [ $tabCount -ge 10000000 ] && [ $tabCount -lt 80000000 ] ; then
			##区间： 1000w ~ 8000w
			resNum=104
		elif [ $tabCount -ge 80000000 ] ; then
			##区间：> 1000w
			resNum=105
		else
			##区间：< 500w
			resNum=101
		fi
	fi
	return $resNum
}


################################################################################
#######HX_CS_QG|CS_DJ_SSXDGJDQGLB|T_TY_HX_CS_DJ_SSXDGJDQGLB|SSXD_DM|NO|159
#######是否为分区表、记录数大小：小表、分区表、非分区大表
#######小表：101
#######分区表：102
#######非分区大表（500~1000）：103
#######非分区大表（>1000）：104
##oracle读取表结构
function oracleRead(){
	local flag=""
	if [ $flag ]; then
		echo "null"

	else
		for line in `cat $confFile | grep -v "^#"`; do
			echo "#####################################################开始######################################################"

			##OWNER
			local owner=`echo $line | awk -F '|' '{print $1}' | tr [a-z] [A-Z]`
			##TABLE_NAME
			local tableName=`echo $line | awk -F '|' '{print $2}' | tr [a-z] [A-Z]`
			##ODPS_TABLE_NAME
			local odpsTable=`echo $line | awk -F '|' '{print $3}' | tr [a-z] [A-Z]`
			##PK
			local splitPk=`echo $line | awk -F '|' '{print $4}' | tr [a-z] [A-Z]`
			##PAR?
			local isParTab=`echo $line | awk -F '|' '{print $5}' | tr [a-z] [A-Z]`
			##TABLE_RECORD
			local tabCount=`echo $line | awk -F '|' '{print $6}'`
			##where
			local whereParam=`echo $line | awk -F '|' '{print $7}'`
			##rz_flag空的时候，程序产生的json用于全量同步，不等于空的，程序产生的json用于增量同步
			if [ "$ZlFlag" == "ZlFlag" ] ; then
				if [ "$whereParam" != "" ] ; then
					whereParam="$whereParam<to_date('\$tmpTime','yyyy-MM-dd:HH24:mi:ss')"
				fi
			else
				#whereParam="$whereParam<to_date('\$tmpTime','yyyy-MM-dd:HH24:mi:ss')"
				whereParam="($whereParam>=to_date('\$bizdate','yyyymmdd'))and($whereParam<to_date('\$bizdate','yyyymmdd')+1)"
			fi
			##查询owner.tablename对应的列名
			local dateTypeSQL="select column_name from all_tab_columns
			where owner=upper('$owner') and table_name=upper('$tableName')
			AND ( DATA_TYPE = 'DATE' OR DATA_TYPE LIKE 'TIMESTAMP%' )
			order by COLUMN_ID;"
			local dateTypeResult=`sqlplus -S $user/$pass@$jdbc <<END
				set heading off
				set feedback off
				set pagesize 0
				set verify off
				set echo off
				$dateTypeSQL
				quit;
END`
				##查询类型
			local tableInfoSQL="select column_name from all_tab_columns
			where owner=upper('$owner') and table_name=upper('$tableName')
			AND DATA_TYPE not in ('LONG', 'NCLOB', 'CLOB', 'BLOB')
			order by COLUMN_ID;"
			local result=`sqlplus -S $user/$pass@$jdbc <<END
				set heading off
				set feedback off
				set pagesize 0
				set verify off
				set echo off
				$tableInfoSQL
				quit;
END`
			local whereParam="where@$whereParam"
			local splitPk="pk:$splitPk"
			#################################  拼装了,"xx" 	--> ID NAME  -->   ,"ID","NAME"
			local sourceSelect=`echo "$result" |awk '{printf ",\""$1"\""}'`
			if [ "$toAsicc" != '' ] ; then
				sourceSelect=`echo "$result" |awk '{printf ",\"convert("$1")\""}'`
				sourceSelect=${sourceSelect//)/,$toAsicc)}
				######打印结果
				#echo "测试1：【${sourceSelect}】"
				for field in `echo $dateTypeResult`
				do
						mField="convert($field,$toAsicc)"
						sourceSelect=`echo $sourceSelect | sed "s/$mField/$field/g"`
						#echo "#####${mField}#####"
				done
						#echo "测试2：【${sourceSelect}】"
			fi
			local targetSelect=`echo "$result" |awk '{printf ",\""$1"\""}'`
			local targetSelect=${targetSelect//,\"RANGE\"/,\"FW\"}
			local jsonTable="$owner"".""$tableName"
			####计算该表所属类型
			calculateTab "$isParTab" $tabCount
			local resNum=$?
			#resNum=102
			echo "#####################返回类型[$resNum]"
			if [ $resNum -eq 101 ] ;then
				echo "##########[$owner.$tableName根据数据量分类为小表]##########"
				trunTableInfo $sourceSelect $targetSelect $odpsTable $jsonTable $whereParam $splitPk "SM" "$channel_sm"
			elif [ $resNum -eq 102 ] ;then
				echo "##########[$owner.$tableName根据数据量分类为分区表]##########"
				partitionSql="select PARTITION_NAME from ALL_TAB_PARTITIONS t where t.table_owner='$owner'and  t.table_name='$tableName';"
				partitionResult=`sqlplus -S $user/$pass@$jdbc <<END
					set heading off
					set feedback off
					set pagesize 0
					set verify off
					set echo off
					$partitionSql
					quit;
END`
				#partitionResult=("aaa bbb ccc")
				##遍历分区值
				for parLine in $partitionResult ; 
				do
					jsonParTable="$owner.$tableName.PARTITION($parLine)"
					echo "$owner.$tableName.PARTITION($parLine)"
					trunTableInfo "$sourceSelect" "$targetSelect" "$odpsTable" "$jsonParTable" "$whereParam" "$splitPk" "PAR" "$channel_fq" "_PARTITION_$parLine"
					jsonParTable=""
				done
			elif [ $resNum -eq 103 ] ;then
				echo "##########[$owner.$tableName根据数据量分类为非分区大表（500w~1000w）]##########"
				trunTableInfo $sourceSelect $targetSelect $odpsTable $jsonTable $whereParam $splitPk "BIG" "$channel_fq"
			elif [ $resNum -eq 104 ] ;then
				echo "##########[$owner.$tableName根据数据量分类为非分区大表（>1000w）]##########"
				trunTableInfo $sourceSelect $targetSelect $odpsTable $jsonTable $whereParam $splitPk "BIG" "$channel_fq"
			elif [ $resNum -eq 105 ] ;then
				echo "##########[$owner.$tableName根据数据量分类为非分区大表（>8000w）]##########"
				trunTableInfo $sourceSelect $targetSelect $odpsTable $jsonTable $whereParam $splitPk "BIG" "$channel_big"
			else			
				echo "##########[$owner.$tableName无效分区]#########"
			fi
		done
	fi
	echo "run is success!"
}

################################################################################
function trunTableInfo(){
	local sourceSelect=$1
	local targetSelect=$2
	local odpsTable=$3
	local tableNames=$4
	local whereList=$5
	local splitPk=$6
	local JsonSecondPath=$7
	local channelNum=$8
	local partitionName=$9
	local jsonName=""
	if [[ $xtype = "R_Z" ]] ; then
		##增量json命名
		jsonName="$odpsTable""${partitionName}_$xtype"
	else
		##全量json命名
		jsonName="$odpsTable""${partitionName}_$xtype"
	fi
	echo "创建$jsonName.json文件"
	echo "datax设置通道数为:[$channelNum]"

	if [ $xtype = "Q" ] ; then
		createQlJsonFile $tableNames ${sourceSelect:1} ${targetSelect:1} $odpsTable $jsonName $whereList $splitPk "$JsonSecondPath" "$channelNum" "$partitionName"
	else
		createZlJsonFile $tableNames ${sourceSelect:1} ${targetSelect:1} $odpsTable $jsonName $whereList $splitPk "$JsonSecondPath" "$channelNum" "$partitionName"
	fi

	echo "#####################################################结束######################################################"
	echo ""
}

function checkDBlink(){
	SQL="select to_char(sysdate,'yyyy-mm-dd') today from dual;"
	ii=0
	flag=false
	DATE=$(date +%Y-%m-%d)
	while [ $ii -lt 3 ]
	do
		OK=`sqlplus -S $user/$pass@$jdbc <<END
			set heading off
			set feedback off
			set pagesize 0
			set verify off
			set echo off
			$SQL
			quit;
END`
			ii=$[ii+1]
		if [[ $OK == $DATE ]] ; then
			flag=true;
			echo "数据库连接连接成功，开始执行脚本!";
			break;
		fi
		sleep 5;
	done
	if [[ $flag == false ]] ; then 
		echo "数据库连接失败，请检查数据库连接信息!";
	else
		################################################################################
		if [[ "$reader" == "mysqlreader" ]];then
			mysqlRead
		elif [[ "$reader" == "oraclereader" ]];then
			oracleRead
		else
			echo "dbType is error：$reader"
		fi
	fi
	exit 0
}

checkDBlink




