#include <stdio.h>

#include "dhmp.h"
#ifndef taillantency 
	#define  obj_num_max  5005
#else
	#define  obj_num_max  505
#endif
// #define octopus 
// #define clover  
// #define L5  
// #define Tailwind  
// #define DaRPC   
// #define scaleRPC
// #define RFP   

const double A = 1.3;  
const double C = 1.0;  

double pf[obj_num_max]; 
int rand_num[obj_num_max]={0};

void show(const char * str, struct timespec* time)
{
#ifdef taillantency
	return;
#endif
	fprintf(stderr,str);	
	fflush(stderr);
	clock_gettime(CLOCK_MONOTONIC, time);	
}

void generate()
{
    int i;
    double sum = 0.0;
 
    for (i = 0; i < obj_num_max; i++)
        sum += C/pow((double)(i+2), A);

    for (i = 0; i < obj_num_max; i++)
    {
        if (i == 0)
            pf[i] = C/pow((double)(i+2), A)/sum;
        else
            pf[i] = pf[i-1] + C/pow((double)(i+2), A)/sum;
    }
}
void pick()
{
	int i, index;

    generate();

    srand(time(0));
    for ( i= 0; i < obj_num_max; i++)
    {
        index = 0;
        double data = (double)rand()/RAND_MAX; 
        while (index<(obj_num_max-6)&&data > pf[index])   
            index++;
		rand_num[i]=index;
    }
}

int main(int argc,char *argv[])
{
	void* addr[obj_num_max];
	int i,j=0,size,rnum;
	int k;
	char m = 0;
	char *str,*str1;
	int objnum,accessnum, write_part;
	struct timespec task_time_start, task_time_end;
	unsigned long task_time_diff_ns;
	char batch;
	uintptr_t local_addr;
	
	if(argc<3)
	{
		printf("input param error. input:<filname> <size> <objnum> <accessnum>\n");
		printf("note:max of <objnum> = max of <accessnum> == 10000\n");
		return -1;
	}
	else
	{
		size=atoi(argv[1]);
		objnum = obj_num_max - 5;//atoi(argv[2]);
		accessnum = 30000;//atoi(argv[3]);
		write_part = atoi(argv[2]);
	}
	pick();
	str=malloc(size+2);
	str1 = malloc(size+2);
	if(!str){
		printf("alloc mem error");
		return -1;
	}
	memset(str, 0 , size);
	snprintf(str, size, "hello world hello");
	snprintf(str1, size, "hhhhhhhhhhhh");
	dhmp_client_init(size,objnum,write_part);

#ifdef octopus
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0); 
	dhmp_malloc(size,10); //for octo
	show("start count",&task_time_start);
	for(;j<accessnum;j++)
	{ 
m = (m+1)%2;
		i = j%objnum;
		model_1_octopus(addr[rand_num[i]], size, str,m);
		
	}

	show("over",&task_time_end);


#endif
#ifdef L5
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	dhmp_malloc(size,3); //for L5

	show("start count",&task_time_start);

	for(;j<accessnum;j++)
	{ 
                i = j%objnum;
m = (m+1)%2;
        model_5_L5(size,str,addr[i],m);
	}

	show(" ",&task_time_end);
#endif
#ifdef FaRM
	batch = BATCH;
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	dhmp_malloc(size,8); //for FaRM
	accessnum = accessnum / batch;
	show("start count",&task_time_start);
j=0;
	for(;j<accessnum;j++)
	{
m = (m+1)%2;
		i = j%objnum;
#ifdef recoverother
		srand(i);
                int suiji = rand()%(100);	
		if(suiji == 99) m=m+10;
#endif
		model_FaRM(str, size, addr[rand_num[i]], m);
	}
	show(" ",&task_time_end);

//	check_request(1);
#endif
#ifdef RFP
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	dhmp_malloc(size,6); //RFP

	show("start count",&task_time_start);

	for(;j<accessnum;j++)
	{ 
		i = j%objnum;
		model_4_RFP(size, str, (uintptr_t)addr[rand_num[i]], i%2); //1 for write
	}

	show(" ",&task_time_end);
#endif
#ifdef scaleRPC
	batch = BATCH;
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	dhmp_malloc(size,7); //scaleRPC
	uintptr_t* temp = malloc(batch*sizeof(uintptr_t));
	local_addr = (uintptr_t)str;
	show("start count",&task_time_start);
	for(;j<accessnum;j=j+batch)
	{ 
	m = (m+1)%2;
		i = j%objnum;
		for(k = 0;k<batch;k++)
			temp[k] = (uintptr_t)(addr[rand_num[(i+k)%objnum]]);
		model_7_scalable(temp, size, &local_addr, i%2 , batch);
	}
	show(" ",&task_time_end);
#endif
#ifdef DaRPC
	//need DaRPC_SERVER at dhmp.h only for server if CQ cluster
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	dhmp_malloc(0,5); //DaRPC
	batch = BATCH;
	uintptr_t* temp = malloc(batch*sizeof(uintptr_t));
	local_addr = (uintptr_t)str;
	show("start count",&task_time_start);
	m = 0;
	for(;j<accessnum;j=j+batch)
	{ 
	m = (m+1)%2;
		i = j%objnum;
#ifdef recoverother 
		srand(i);
                int suiji = rand()%(100);
                if(suiji == 99) m=m+10;
#endif
		for(k = 0;k<batch;k++)
			temp[k] = (uintptr_t)(addr[rand_num[(i+k)%objnum]]);
		model_3_DaRPC(temp, size, &local_addr, m , batch); //用的默认的send recv queue
	}

	show(" ",&task_time_end);
#endif
#ifdef FaSST
//need #define UD
	for(i=0;i<objnum;i++)
		addr[i]=dhmp_malloc(size,0);
	batch = 1;
	uintptr_t* temp = malloc(batch*sizeof(uintptr_t));
	local_addr = (uintptr_t)str;
	show("start count",&task_time_start);
	for(;j<accessnum;j=j+batch)
	{ 
		i = j%objnum;
		for(k = 0;k<batch;k++)
			temp[k] = (uintptr_t)addr[rand_num[(i+k)%objnum]];
		send_UD(temp, size, &local_addr , i%2 , batch); //用的默认的send recv queue
	}
	check_request(3);
	show(" ",&task_time_end);
#endif

 task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec) -((task_time_start.tv_sec * 1000000000) + task_time_start.tv_nsec);
	
//	task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec);
  	fprintf(stderr,"%lf\n",(double)task_time_diff_ns/1000000);
	fflush(stderr);

	//	for(i=0;i<objnum;i++)	
	//	dhmp_free(addr[i]);
	dhmp_client_destroy();

	
	return 0;
}



