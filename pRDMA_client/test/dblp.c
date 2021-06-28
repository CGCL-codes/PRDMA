#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include "dhmp.h"
#define NODE_MAX 326186
#define EDGE_MAX 1615400
#define node_num 326186

struct edge	
{
	int u;
	int v;
};
 

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
//model_FaRM(str,size,addr[rand_num[i]],1);
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
	struct timespec task_time_start, task_time_end;
	unsigned long task_time_diff_ns;
	int ncnt=1;
	int ecnt=0;
	double eps=0.1;
	int a,u,v;
	int i,count ;
	char StrLine[2048];

	FILE *fp = fopen("/home/hdlu/dblp-2010","r");
	dhmp_client_init(sizeof(struct parad ),0,0);
size_t size= sizeof(struct parad );
//	dhmp_malloc(sizeof(struct parad ), 10);
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

	for(i = 0;i<EDGE_MAX/10;i++)
	{
		para[i] =(struct parad *) dhmp_malloc(sizeof(struct parad),0);
	}
fprintf(stderr,"star2\n");
fflush(stderr);
	i = 0;u=0;
	for(count=0;count<node_num;++count)
	{	
		if(feof(fp)) break;
        fgets(StrLine,2048,fp);  
        {
         	int ti = 0;
         	int len = strlen(StrLine)-1;
         	while(ti<len)
         	{
         		v = 0;
         		while(StrLine[ti]<='9' && StrLine[ti] >= '0')
         		{
         			v = v*10 + StrLine[ti] - '0';
         			ti++;
         		}
         		{
					 printf("2i = %d\n",i);
					amper_write(para[i/10], tpara, sizeof(struct parad));	
					edge[i].u = u;
					edge[i].v = v;
	amper_read(para[i/10], tpara, sizeof(struct parad));
					i++;	
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

         		ti++;
         	}
        }
        u++;
	}
	ecnt=i;
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
		
			amper_read(para[i/10], tpara, sizeof(struct parad));	
			u = edge[i].u;
			v = edge[i].v;
			rb[rednum[v]]+=ra[rednum[u]]/d[rednum[u]]; 
		}
		for(i=0;i<ncnt;++i)
		{
			amper_read(para[i/10], tpara, sizeof(struct parad));	
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


