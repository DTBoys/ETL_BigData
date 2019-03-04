#!/bin/bash
################################################################################
# 脚本名称       : ty_commit_datax_zl.sh 提交全量表json的datax任务
#
# 创建日期       : 2017/08/31
# 作者           : 刘志雄
#
# 描述           : 提交datax任务上云
# 参数描述       : 1、日期 bizDate
#		           2、项目名称如HX
#                  3、任务名称jobname，即odps表名
#
# 修改记录      :
#
#    日期    操作           操作人员                      描述		
# ________  _______   __________________    __________________________________
#
# 2017/08/31  创建    刘志雄     
################################################################################
if [ $# -lt 3 ] ; then
   echo  "请输入正确的参数:1、业务日期 2、项目名称 3、任务名称"
   exit 1;
fi

bizDate=$1
syscode=`echo $2| tr [a-z] [A-Z]`
taskname=`echo $3| tr [a-z] [A-Z]|awk -F '_R_Z' '{print $1}'`
taskname="${taskname}_DSV_R_Z"
logTime=`date '+%H%M%S'`
##根据获取当前shell脚本路径，设置其他配置文件路径
#dataxHome="/data/datax"
dataxHome=/home/admin/datax3
##基础路径 /hyht/version/ty
#base=/home/version/TY
base=/home/admin/version/TY
outpath=${base}/shell/log/commit/`date '+%Y%m%d'`/${syscode}
if [ ! -d  ${outpath} ];then
	mkdir -p ${outpath}
fi

if [ ! -d  ${outpath}/succLog ];then
	mkdir -p ${outpath}/succLog
fi

if [ ! -d  ${outpath}/failLog ];then
	mkdir -p ${outpath}/failLog
fi
source ${base}/${syscode}/conf/ty_datasource.conf

##判断FTP服务器dsv文件是否存在
##参数 1、名称  2、业务日期  3、表名:查找对应的dsv文件
##定义全局变量,默认0文件不存在
flag=0
function rmtFileIsExist(){
syscode=$1
bizDate=$2
tbName=$3

ret=`ftp -inv ${ftpIp}  <<!
user ${ftpUser} ${ftpPasswd}
ls /home/DSV/${syscode}/${bizDate}/*.${tbName}.*.dsv
quit
!`
echo "===测试：$ret"
str1=`echo ${ret}|grep "Login successful"`
str2=`echo ${ret}|grep ${tbName}`
##登陆失败为-1,登陆成功文件存在为1
if [[ ! ${str1} ]]; then
	flag=-1
elif [[ ${str2} ]]; then
	flag=1
fi
}

##全量json目录
zlJsonPath="${base}/${syscode}/json/DSV"

##组装dataX 动态参数
datxD="-Dbizdate=${bizDate} -DftpIp=${ftpIp} -DftpPort=${ftpPort} -DftpUser=${ftpUser} -DftpPasswd=${ftpPasswd} -DftpBasePath=${ftpBasePath} -DodpsaccessId=${odpsaccessId} -DodpsaccessKey=${odpsaccessKey} -Dproject=${project}"
##提交datax任务，并获取执行结果
jsonName=`echo ${taskname} | tr [a-z] [A-Z]`
for file in `ls ${zlJsonPath}/*.json|grep "${jsonName}\.json"` ;do
	jsonName="${file##*/}"
	fileName="${jsonName%%.*}"
	#判断是否存在对应的dsv文件，如果不存在，则不执行
	tbName=`echo ${jsonName} |awk -F "T_TY_${syscode}_" '{print $2}'|awk -F '_DSV_R_Z' '{print $1}'`
	rmtFileIsExist "${syscode}" "${bizDate}" "${tbName}"
	if [ ${flag} -eq -1 ] ;then
		exit 1
	fi
	if [ ${flag} -eq 0 ] ;then
		continue
	fi
	echo "${file}" >> ${outpath}/commitJson.log
	echo "开始执行${jsonName}"
	python ${dataxHome}/bin/datax.py --jvm="-Xms4g -Xmx4g" ${file} -p "${datxD}" 2>&1 >> ${outpath}/${fileName}_${logTime}.log
	result=$?
	if [ ${result} -eq 0 ] ;then
		echo "${fileName}" >> ${outpath}/succJson.log
		mv -f ${outpath}/${fileName}_${logTime}.log ${outpath}/succLog
	else
		echo "${fileName}" >> ${outpath}/failJson.log
		mv -f ${outpath}/${fileName}_${logTime}.log ${outpath}/failLog
		exit 1
	fi
done
exit 0
