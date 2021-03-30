ps -ef|grep client |grep -v grep|cut -c 9-15|xargs kill -9
