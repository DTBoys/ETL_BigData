#!/bin/bash

################################################################################
# SCRIPT_NAME     : ty_create_table_sql.sh
#
# CREATE_TIME     : 2018/04/10
# AUTHOR          : Mochou_liqb
#
# DESCRIBETION    : get create_table_sql
# PARAMETER       : 1 baseConf, such as HX
# EXAMPLE         : ./ty_create_table_sql.sh HX
# 
# UPDATE_RECORD   : change comments from Chinese to English and kill some bugs
#
# DATE      OPERATION       CZR         DESCRIBETION              		
# ________  _____________   ________    __________________________________
#
# 2018/05/26  UPDATE Mochou_liqb  
################################################################################


if [ $# != 1 ] ; then 
echo "USAGE: baseConf IS NULL" 
exit 1; 
fi 
##SYSCODE_NAME  such as HX
baseConf="${1}"


###############################################################################
####################################PUBLIC VARCIBLES###################################
##making some import dirs
##json base dirs
versionBase="/home/admin/version/TY"
jsonBase="${versionBase}/${baseConf}/json"
##sql base dirs
sqlBase="${versionBase}/sql"
sqlBaseBak="${versionBase}/sql/bak"
##conf base dirs
confBase="${versionBase}/${baseConf}/conf"
##shell base dirs
shellBase="${versionBase}/shell"
##配置文件目录，如：HXZG
splitF="#"
##数据库文件
source ${confBase}/ty_datasource.conf
##版本
curdt="`date +%Y%m%d%H%M%S`"
reader="oraclereader" 
##抽取类型，增量、全量
#xtype="R_Z"
xtype="Q"
################################################################################



################################################################################
##############oracle sqlplus环境变量 根据服务器的ORACLE_HOME目录配置############
if [[ "${reader}" == "oraclereader" ]];then 
export ORACLE_HOME=${TY_ORACLE_HOME}
export LD_LIBRARY_PATH=${TY_LD_LIBRARY_PATH}
export NLS_LANG="${nls_lang}"
export PATH=${ORACLE_HOME}/bin:${LD_LIBRARY_PATH}:${PATH}
fi


##目录检测
if [ ! -d ${jsonBase} ] ;then
	mkdir -p ${jsonBase}
fi
if [ ! -d ${sqlBase} ] ;then
	mkdir -p ${sqlBase}
fi
if [ ! -d ${sqlBaseBak} ] ;then
	mkdir -p ${sqlBaseBak}
fi
if [ ! -d ${confBase} ] ;then
	mkdir -p ${confBase}
fi



################################################################################
##oracle
function oracleRead(){
##剪切为历史文件
if [ -f "${sqlBase}/${baseConf}_Create_Table.sql" ]; then
 mv "${sqlBase}/${baseConf}_Create_Table.sql" "${sqlBaseBak}/${baseConf}_Create_Table_${curdt}.sql"
fi
for line in `cat ${confBase}/ty_createJson_ql.conf | grep -v "^#"`; do
##替换花括号
tablestr=`echo $line | awk -F '|' '{print $2}' | sed -r "s/\(|\)//g" | tr [a-z] [A-Z]`
tableUser=`echo $line | awk -F '|' '{print $1}' | sed -r "s/\(|\)//g" | tr [a-z] [A-Z]`
odpsTable=`echo $line | awk -F '|' '{print $3}' | sed -r "s/\(|\)//g" | tr [a-z] [A-Z]`
echo "开始：${tablestr}---${tableUser}"
loadsql="
select lower(ODPS_COLUMN) odps_colum
  from (SELECT T.TABLE_NAME,
               CASE
                 WHEN T.COLUMN_ID = 1 THEN
                  '(' || T.COLUMN_NAME || ' ' || T.ODPSTYPE || ' COMMENT ''' ||
                  T.COL_COMMENT || ''','
                 ELSE
                  T.COLUMN_NAME || ' ' || T.ODPSTYPE || ' COMMENT ''' ||
                  T.COL_COMMENT || ''','
               END AS ODPS_COLUMN,
               T.COLUMN_ID,T.OWNER
          FROM (select A.TABLE_NAME,
                       --A.COLUMN_NAME,
					   decode(upper(A.COLUMN_NAME),'RANGE','FW',upper(A.COLUMN_NAME)) COLUMN_NAME,
                       case
                         when (a.DATA_TYPE in ('CHAR', 'VARCHAR2', 'VARCHAR')) OR
                              (A.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND
                              A.data_precision >= 19) then
                          'string'
                         when (a.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE = 0 AND
                              A.data_precision < 19) OR
                              (A.DATA_TYPE = 'NUMBER' AND A.DATA_LENGTH = 22 and
                              A.DATA_PRECISION IS NULL AND A.DATA_SCALE = 0) THEN
                          'bigint'
                         when A.DATA_TYPE IN
                              ('BINARY_FLOAT', 'BINARY_DOUBLE', 'FLOAT') then
                          'double'
                         when a.DATA_TYPE in ('DATE', 'TIMESTAMP(6)') then
                          'datetime'
                         when (a.DATA_TYPE = 'NUMBER' AND A.DATA_SCALE > 0 and
                              A.data_precision - A.DATA_SCALE <= 36 and
                              A.DATA_SCALE <= 18) then
                          'decimal'
                         when a.data_type = 'BOOLEAN' then
                          'boolean'
                         else
                          'string'
                       end as odpstype,
                       B.COMMENTS COL_COMMENT,
                       D.MAX_ID,
                       A.COLUMN_ID,
                       A.OWNER OWNER
                  from all_tab_cols a,
                       (SELECT MAX(MA.COLUMN_ID) MAX_ID, TABLE_NAME
                          FROM all_tab_cols MA
                         GROUP BY TABLE_NAME) D,
                       all_col_comments B
                 where B.COLUMN_NAME(+) = A.COLUMN_NAME
                   AND B.TABLE_NAME(+) = A.TABLE_NAME
                   AND D.TABLE_NAME = A.TABLE_NAME
                   and a.OWNER= upper('${tableUser}') AND B.OWNER=A.OWNER
                   AND A.DATA_TYPE not in ('LONG', 'NCLOB', 'CLOB', 'BLOB')) T
        UNION all
        select A.TABLE_NAME,
              'CREATE TABLE IF NOT EXISTS ${odpsTable}' ODPS_COLUMN,
               0 COLUMN_ID,a.owner
          from all_tables a
        UNION all
        select a.TABLE_NAME,
               'YPT_JGSJ datetime comment ''云平台数据加工时间'',' ODPS_COLUMN,
               1000 COLUMN_ID,a.owner
          from all_tables a
        UNION all
		select a.TABLE_NAME,
               'ypt_ysjczlx string comment ''源数据操作类型'',' ODPS_COLUMN,
               1001 COLUMN_ID,a.owner
          from all_tables a
        UNION all
		select a.TABLE_NAME,
               'ypt_ysjczsj datetime comment ''源数据操作时间'',' ODPS_COLUMN,
               1002 COLUMN_ID,a.owner
          from all_tables a
        UNION all
        select a.TABLE_NAME,
               'ypt_ysjczxl string comment ''源数据操作序列'')' ODPS_COLUMN,
               1003 COLUMN_ID,a.owner
          from all_tables a
        UNION all
        select A.TABLE_NAME,
               'COMMENT ''' || A.COMMENTS || '''' ODPS_COLUMN,
               1004 COLUMN_ID,a.owner
          from all_tab_comments a
        UNION all
        select A.TABLE_NAME,
               'PARTITIONED BY (RFQ STRING COMMENT ''日分区'');' ODPS_COLUMN,
               1005 COLUMN_ID,a.owner 
          from all_tables a ) P
 where Table_Name = upper('${tablestr}')
   and owner = upper('${tableUser}')
 order by TABLE_NAME, column_id;
"
echo "【====================================检测完毕==================================】"
echo "【库表检测：${tableUser}.${tablestr%%)*}】"
##初始化脚本，版本控制判断
tableInfoSQL="${loadsql}"
result=`sqlplus -S ${user}/${pass}@${jdbc} <<END
set heading off
set feedback off
set pagesize 0
set verify off
set echo off
set line 3000
${tableInfoSQL}
quit;
END`
sselect=`echo  "${result}"| awk  '{printf "%s\n", $0}'`
echo "生成${tableUser}.${tablestr}建表语句"
echo "${sselect}">>${sqlBase}/${baseConf}_Create_Table.sql
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
set line 3000
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
oracleRead
fi
exit 0
}

checkDBlink

