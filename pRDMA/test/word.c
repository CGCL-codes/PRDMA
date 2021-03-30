#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include "dhmp.h"
#define NODE_MAX 12000
#define EDGE_MAX 75000
#define node_num 10617
struct edge	
{
	int u;
	int v;
}obj_epara;
 

struct parad 
{
	int rednum;
	int d;
	double ra,rb;
	int v,u;
}; 

int rednum[NODE_MAX];	
int d[NODE_MAX];		
double ra[NODE_MAX];		
double rb[NODE_MAX];
struct edge		edge[EDGE_MAX];


void  *para[EDGE_MAX];
struct parad obj_para;


void amper_write(void* globle_addr, void*local_addr, size_t  size)
{
uintptr_t globle,local;
globle = globle_addr;
local = local_addr;
#ifdef FaRM
model_FaRM(local_addr, size,globle_addr ,1);
#endif
#ifdef scaleRPC
model_7_scalable(&globle, size, &local,1 , 1);
#endif
#ifdef DaRPC
	model_3_DaRPC(&globle, size, &local,1 , 1);
#endif
#ifdef octopus
	model_1_octopus(globle_addr, size, local_addr,1);
#endif
#ifdef L5
model_5_L5(size, local_addr,globle_addr, 1);
#endif
#ifdef RFP
model_4_RFP(size, local_addr,globle_addr, 1);
#endif
}


void amper_read(void* globle_addr, void*local_addr, size_t  size)
{
uintptr_t globle,local;
globle = globle_addr;
local = local_addr;
#ifdef RFP
model_4_RFP(size, local_addr,globle_addr, 0);
#endif

#ifdef FaRM
model_FaRM(local_addr, size,globle_addr ,0);
#endif
#ifdef scaleRPC
model_7_scalable(&globle, size, &local,0 , 1);
#endif
#ifdef DaRPC
        model_3_DaRPC(&globle, size, &local,0 , 1);
#endif
#ifdef L5
model_5_L5(size, local_addr,globle_addr, 0);
#endif
#ifdef octopus
	model_1_octopus(globle_addr, size, local_addr,0);
#endif
}
void pagerank()
{
	struct parad *tpara = &obj_para;
	struct edge * tepara = &obj_epara;
	struct timespec task_time_start, task_time_end;
	unsigned long task_time_diff_ns;
	int ncnt=1;
	int ecnt=0;
	double eps=0.1;
	int a,u,v;
	int i,count ;
	char StrLine[1024];

	FILE *fp = fopen("/home/hdlu/wordassociation-2011","r");
	dhmp_client_init(sizeof(struct parad ),0,0);
	dhmp_malloc(sizeof(struct parad ), 10);
size_t size= sizeof(struct parad );

#ifdef RFP
        dhmp_malloc(size,6);
#endif

#ifdef FaRM
        dhmp_malloc(size,8); //for FaRM
#endif

#ifdef scaleRPC
        dhmp_malloc(size,7);
#endif
#ifdef DaRPC
        dhmp_malloc(0,5);
#endif
#ifdef L5
        dhmp_malloc(size,3);
#endif
#ifdef octopus
        dhmp_malloc(sizeof(struct parad ), 10);
#endif


	clock_gettime(CLOCK_MONOTONIC, &task_time_start);

	for(i = 0;i<EDGE_MAX;i++)
	{
		para[i] =(struct parad *) dhmp_malloc(sizeof(struct parad),0);
	}
printf("star2\n");
	for(i=0;;++i)
	{	
		if(feof(fp)) break;
		fscanf(fp,"%d %d",&u,&v);
			printf("i = %d\n",i);
		edge[i].u = u;
		edge[i].v = v;
	amper_write(para[i], tpara, sizeof(struct parad));	
	}
	ecnt=i;
	printf("star3\n");

	
	for(i=0;i < ecnt;++i)
	{
	 printf("2i = %d\n",i);
amper_read(para[i], tpara, sizeof(struct parad));	
		u = edge[i].u;
		v = edge[i].v;	
		if(!rednum[u])
		{
			rednum[u]=ncnt;
			ncnt++;
		}
		if(!rednum[v]) 
		{
			rednum[v]=ncnt;
			ncnt++;
		}
		d[rednum[u]]++;
	}
	ncnt--;
	for(i=0;i<ncnt;++i)
	{
	 printf("3i = %d\n",i);
		ra[i]=(double)1/ncnt;
	}
	printf("ncnt ==%d %d\n",ncnt,ecnt);


	while(eps>0.0000001)
	{
		printf("eps%.7lf\n",eps);
		eps=0;
		for(i=0;i<ecnt;++i)
		{
		
			amper_read(para[i], tpara, sizeof(struct parad));	
			u = edge[i].u;
			v = edge[i].v;
			rb[rednum[v]]+=ra[rednum[u]]/d[rednum[u]]; 
		}
		for(i=0;i<ncnt;++i)
		{
			rb[i]=rb[i]*0.8+(0.2*1/ncnt);
			eps+=ra[i]>rb[i]?(ra[i]-rb[i]):(rb[i]-ra[i]);
			ra[i]=rb[i];
			rb[i]=0;
		}

	}
	clock_gettime(CLOCK_MONOTONIC, &task_time_end);
	
	task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec) -
                        ((task_time_start.tv_sec * 1000000000) + task_time_start.tv_nsec);

	printf("runtime %lf\n", (double)task_time_diff_ns/1000000);
	 dhmp_client_destroy();
}

int main()
{
	pagerank();
	return 0;
}


