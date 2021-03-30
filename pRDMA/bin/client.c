#include <stdio.h>

#include <time.h>
void show(const char * str, struct timespec* time)
{
	clock_gettime(CLOCK_MONOTONIC, time);	
}


int main(int argc,char *argv[])
{
	struct timespec task_time_start, task_time_end;
	unsigned long task_time_diff_ns;
	show("start count",&task_time_start);
	sleep(0);
	show(" ",&task_time_end);

 task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec) -((task_time_start.tv_sec * 1000000000) + task_time_start.tv_nsec);
	
//	task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec);
  	fprintf(stderr,"%lf\n",(double)task_time_diff_ns/1000000);
	fflush(stderr);

	//	for(i=0;i<objnum;i++)	
	//	dhmp_free(addr[i]);

	
	return 0;
}



