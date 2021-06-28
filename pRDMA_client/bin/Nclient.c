#include <time.h>
#include <stdio.h>
#include<stdlib.h>
#include <unistd.h>
#define NUM 10
int main(){
  struct timespec task_time_start, task_time_end;
        unsigned long task_time_diff_ns;
  int i = 0;
	clock_gettime(CLOCK_MONOTONIC,&task_time_end);
	 task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec);
	fprintf(stderr,"\n**********************************************\n\n\tStart time = %lfms\n\n**********************************************\n\n\n",(double)task_time_diff_ns/1000000);
//        fprintf(stderr,"start time : %lf\n", (double)task_time_diff_ns/1000000);
        fflush(stderr);
  for(i = 0;i<NUM;i++){
    if(0 == fork()){
      execl("./client","./client","65536","5",NULL);
      exit(0);
    }
  } 
  return 0;
}
