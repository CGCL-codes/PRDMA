#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>

#define NODE_MAX 12000
#define EDGE_MAX 75000
#define node_num 10617
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
	int u,v;
}; 

int rednum[NODE_MAX];	
int d[NODE_MAX];		
double ra[NODE_MAX];		
double rb[NODE_MAX];
struct edge		edge[EDGE_MAX];


void  *para[NODE_MAX];
struct parad *tpara;
void pagerank()
{
	
	struct timespec task_time_start, task_time_end;
	unsigned long task_time_diff_ns;
	int ncnt=1;
	int ecnt=0;
	double eps=0.1;
	int a,u,v;
	int i,count ;
	char StrLine[1024];

	FILE *fp = fopen("/home/lhd/wordassociation-2011","r");
	dhmp_client_init(sizeof(struct parad ),0,0);
	dhmp_malloc(sizeof(struct parad ), 10);

	clock_gettime(CLOCK_MONOTONIC, &task_time_start);


	for(i = 0;i<NODE_MAX;i++)
	{
		para[i] = dhmp_malloc(sizeof(struct parad),0);
	}

	for(i=0;;++i)
	{	
		if(feof(fp)) break;
		fscanf(fp,"%d %d",&u,&v);
		edge[i].u = u;
		edge[i].v = v;
		tpara->u = u;
		tpara->v = v;
		amper_write(para[i], tpara sizeof(struct parad));	
	}
	ecnt=i;
	

	
	for(i=0;i < ecnt;++i)
	{	
		amper_read(para[i], tpara, sizeof(struct parad));	
		u = edge[i].u;
		v = edge[i].v;	
		amper_read(para[u], tpara, sizeof(struct parad));	
		if(!rednum[u])
		{
			rednum[u]=ncnt;
			amper_write(para[u], tpara sizeof(struct parad));	
			ncnt++;
		}
		amper_read(para[v], tpara, sizeof(struct parad));	
		if(!rednum[v]) 
		{
			rednum[v]=ncnt;
			amper_write(para[v], tpara sizeof(struct parad));	
			ncnt++;
		}
		amper_read(para[rednum[u]], tpara, sizeof(struct parad));	
		d[rednum[u]]++;
		amper_write(para[rednum[u]], tpara, sizeof(struct parad));	
	}
	ncnt--;
	for(i=0;i<ncnt;++i)
	{
		ra[i]=(double)1/ncnt;
		amper_write(para[i], tpara, sizeof(struct parad));	
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
			amper_read(para[u], tpara, sizeof(struct parad));	
			amper_read(para[v], tpara, sizeof(struct parad));	
			amper_read(para[rednum[u]], tpara, sizeof(struct parad));	
			amper_read(para[rednum[v]], tpara, sizeof(struct parad));	
			rb[rednum[v]]+=ra[rednum[u]]/d[rednum[u]]; 
			amper_write(para[rednum[v]], tpara, sizeof(struct parad));	
		}
		for(i=0;i<ncnt;++i)
		{
			amper_read(para[i], tpara, sizeof(struct parad));	
			rb[i]=rb[i]*0.8+(0.2*1/ncnt);
			amper_write(para[i], tpara, sizeof(struct parad));	
			eps+=ra[i]>rb[i]?(ra[i]-rb[i]):(rb[i]-ra[i]);
			ra[i]=rb[i];
			rb[i]=0;
			amper_write(para[i], tpara, sizeof(struct parad));	
		}

	}
	clock_gettime(CLOCK_MONOTONIC, &task_time_end);
	
	task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec) -
                        ((task_time_start.tv_sec * 1000000000) + task_time_start.tv_nsec);
	for(i=0;i<ncnt;++i)
	{
		fprintf(fp1,"%d %lf\n",i,ra[rednum[i]]);
	}
	dhmp_client_destroy();

	printf("runtime %lf\n", (double)task_time_diff_ns/1000000);
}

int main()
{
	pagerank();
	return 0;
}


//320004.209643


memset(server->rednum , 0,NODE_MAX * sizeof(int));
memset(server->d ,0,NODE_MAX * sizeof(int));
memset(server->ra ,0,NODE_MAX * sizeof(double));
memset(server->rb ,0,NODE_MAX * sizeof(double));
memset(server->edge,0, EDGE_MAX * sizeof(struct edge));
