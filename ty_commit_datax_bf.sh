#!/bin/bash
################################################################################
# SCRIPT_NAME     : ty_commit_datax_bf.sh
#
# CREATE_TIME     : 2018/04/10
# AUTHOR          : ZhiXiong Liu/JiangWei Yu
#
# DESCRIBETION    : comparing today's and yesterday's all_datas to get changingdatas
# PARAMETER       : 1 baseConf, such as HX
#                   2 processNum
#					3 RFQ
# EXAMPLE         : ./ty_commit_datax_bf.sh HX 10 00000000
#
# UPDATE_RECORD   : change comments from Chinese to English and kill some bugs
#
# DATE      OPERATION       CZR         DESCRIBETION
# ________  _____________   ________    __________________________________
#
# 2018/05/26  UPDATE Mochou_liqb
################################################################################
if [ $# -lt 2 ] ; then
	echo  "请输入正确的参数:1、项目名称 2、并发任务数 3、业务日期(可选，默认为当天日期的前一天) "
	exit 1;
fi
syscode=$1
processNum=$2
bizDate=$3
##基础路径
base=/home/admin/version/TY
##老版本的datax
#dataxHome=/home/admin/datax3
##新版本的datax
dataxHome=/home/admin/datax/datax3
##配置文件目录
confBase="$base/$syscode/conf"
source $confBase/ty_datasource.conf
##全量json目录
qlJsonPath="$base/$syscode/json/Q"
##如果统计表记录数文件不存在，需先执行统计表记录数脚本
#if [ ! -f ${shellPath}/log/count/${syscode}_CountTable.conf ] ;then
#	echo "表记录数文件${syscode}_CountTable.conf不存在，请先执行统计记录数脚本!"
#	exit 1;
#fi
startTime=`date '+%Y%m%d%H%M%S'`
##输出日志路径
outpath="$base/shell/log/commit/$syscode/$startTime"
if [ -d $outpath ] ;then
	rm -rf  $outpath
fi
mkdir -p  $outpath
touch $outpath/commitJson.log
touch $outpath/succJson.log
touch $outpath/failJson.log
mkdir -p $outpath/succLog
mkdir -p $outpath/failLog
jsonNum=`ls $qlJsonPath/*/*.json | wc -l`
if [ $processNum -gt $jsonNum ] ;then
	echo "并发任务数必须小于等于json文件数:$jsonNum"
	exit 1;
fi

##以当前进程号命名
fifoName="/tmp/$$.fifo"
mkfifo $fifoName
##定义文件描述符(fd是一个整数，可理解为索引或指针),以读写的方式绑定到文件描述符3中
exec 3<>"$fifoName"
##此时可以删除管道文件，保留fd即可
rm -rf $fifoName
##清空补数据的conf文件
echo "" > $confBase/ty_createJson_bsj.conf
##定义进程数量,向管道文件中写入进程数量个空行
for ((i=1;i<=$processNum;i++)) do
	echo >&3
done

##业务日期为空 则 获取当前时间去前一天日期
if [[ ! $bizDate ]];then
	bizDate=`date '+%Y%m%d' -d "-1 days"`
fi

##组装dataX 动态参数
datxD="-Dbizdate=$bizDate -DodpsaccessId=$odpsaccessId -DodpsaccessKey=$odpsaccessKey -Dproject=$project -DodpsJdbc=$odpsJdbc -Duser=$user -Dpass=$pass"

##根据配置文件ty_createJson.conf中的表匹配json目录下的json文件，如果一个表匹配到多个json文件(分区表的情况)，则依次提交
for line in `cat $confBase/ty_createJson_ql.conf | grep -v "^#"`; do
	odpsTb=`echo $line | awk -F '|' '{print $3}'`
	for file in `ls $qlJsonPath/*/*.json|grep -E "${odpsTb}_Q\.json|${odpsTb}_PARTITION_.*_Q\.json"` ;do
		##读入一个空行
		read -u3
		{
			jsonName="${file##*/}"
			fileName="${jsonName%%.*}"
			echo $jsonName
			echo "$file" >> $outpath/commitJson.log
			python $dataxHome/bin/datax.py $file -p "$datxD" 2>&1 >> $outpath/$fileName.log
			result=$?
			if [ $result -eq 0 ] ;then
				echo "$fileName" >> $outpath/succJson.log
				mv -f $outpath/$fileName.log $outpath/succLog
			else
				echo "$fileName" >> $outpath/failJson.log
				mv -f $outpath/$fileName.log $outpath/failLog
				##记录失败的表配置
				echo "$line" >> $confBase/ty_createJson_bsj.conf
			fi
			sleep 1
			##最后向管道文件中写入一个空行
			echo >&3
		}&
	done
done
wait
echo "开始时间:$startTime"  >> $outpath/commitJson.log
echo "开始时间：$startTime"
endTime=`date '+%Y%m%d%H%M%S'`
echo "结束时间:$endTime" >> $outpath/commitJson.log
echo "结束时间:$endTime"
#关闭文件描述符的读
exec 3<&-
#关闭文件描述符的写
exec 3>&-
exit 0
