#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include "dhmp.h"

#define totol_size 70000
#define key_size (sizeof(void*))
#define value_size (1024)

#define  obj_num  50000

#define accessNum 300000

const double A = 1.3;  
const double C = 1.0;  
int j;
double pf[obj_num]; 
int rand_num[obj_num]={0};
int now_num;

void * addr_table[totol_size]; 
void* meta[totol_size];
void* local_buf;

void* temp[100];

void generate()
{
    int i;
    double sum = 0.0;
 
    for (i = 0; i < obj_num; i++)
        sum += C/pow((double)(i+2), A);

    for (i = 0; i < obj_num; i++)
    {
        if (i == 0)
            pf[i] = C/pow((double)(i+2), A)/sum;
        else
            pf[i] = pf[i-1] + C/pow((double)(i+2), A)/sum;
    }
}
void pick(int max_num)
{
	int i, index;

    generate();

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        index = 0;
        double data = (double)rand()/RAND_MAX; 
        while (index<(max_num)&&data > pf[index])   
            index++;
		rand_num[i]=index;
    }
}


void transport(void * remote_addr, void* buf_addr, size_t size , char flag)
{
//printf("transport %p %d %d\n",remote_addr,size,flag);
	uintptr_t globle,local;
	globle = (uintptr_t) remote_addr;
	local = (uintptr_t)buf_addr;
#ifdef FaRM
	model_FaRM(local, size,globle ,flag);
#endif
#ifdef scaleRPC
	model_7_scalable(&globle, size, &local,flag , 1);
#endif
#ifdef DaRPC
    model_3_DaRPC(&globle, size, &local, flag , 1);
#endif
#ifdef octopus
     model_1_octopus(remote_addr, size, buf_addr,flag);
#endif
#ifdef L5
	model_5_L5(size, buf_addr,remote_addr, flag);
#endif
#ifdef RFP
	model_4_RFP(size, buf_addr,remote_addr, flag);
#endif

#ifdef LOCAL
	if(flag == 1)
		memcpy(addr_table_addr ,buf_addr, size);
	else
		memcpy(buf_addr ,addr_table_addr, size);
#endif
	return;
}

unsigned int time33(void *key){
	
    unsigned int hash = 5381;
  int i = 3;
    while(i--){
        hash += (hash << 5 ) +(uint64_t) key;
    }
    return (hash & 0x7FFFFFFF) %totol_size;
}


void addr_table_insert(void* key,void * value)
{
	int curcla = 0;
	unsigned int index = time33(key );
	while(meta[index] != 0)
	{
		index++;
		curcla++;
		if(curcla > totol_size)
		{
			printf("eror no space\n");
			return;
		}
		if(index > totol_size){
			index = 0;
		}
	}
	meta[index] = key;
	transport(meta[index] ,value, value_size,1);
}

void addr_table_update(void * key,void *value)
{
	unsigned int index = time33(key );
	while(meta[index] != key)
	{
		if(meta[index] == NULL)
		{
			printf("eror no find update\n");
			return;
		
		}
		index ++;

		if(index > totol_size){
			index = 0;
		}
	}
	transport(meta[index] ,value, value_size,1);
}

void addr_table_read(void *key,void * buf)
{
	unsigned int index = time33(key );
	while(meta[index] != key)
	{
		//printf("////startead %d %p\n",index,key);
		if(meta[index] == NULL)
		{
			printf("eror no find read %d\n",index);
			return;
		}
		index ++;
		if(index > totol_size){
			index = 0;
		}
	}
	transport( meta[index] ,buf , value_size,0);
	return ;
}


void addr_table_scan( int count,void * buf)
{
	int i;
	for(i=0; i < count;i++)
	{
		addr_table_read(temp[i], buf/* + i*value_size*/);
	}
	return ;
}

void workloada()
{
	int i = 0;
	pick(obj_num);
	int read_num,update_num;
	read_num = accessNum /2;
	update_num = accessNum /2;
	for(;i < accessNum ;i++)
	{
		j = i % obj_num;
		srand(i);
		int suiji = rand()%(read_num+update_num);
		{
			if(suiji < read_num)
			{
				read_num--;
				addr_table_read(addr_table[rand_num[j]],local_buf);
		
			}
			else
			{
				update_num --;
				addr_table_update(addr_table[rand_num[j]],local_buf);
			}
		}
		
	}
}

void workloadb()
{
	int i=0;
	pick(obj_num);
	int read_num,update_num;
	read_num = accessNum *0.95;
	update_num = accessNum *0.05;
	for(;i < accessNum ;i++)
	{
		j = i % obj_num;
		srand(i);
		int suiji = rand()%(read_num+update_num);
		{
			if(suiji < read_num)
			{
				read_num--;
				addr_table_read(addr_table[rand_num[j]],local_buf);
		
			}
			else
			{
				update_num --;
				addr_table_update(addr_table[rand_num[j]],local_buf);
			}
		}
		
	}
}

void workloadc()
{
	int i=0;
	pick(obj_num);
	for(i = 0;i < accessNum ;i++)
	{
		//printf("workc = %d\n",i);
		j = i % obj_num;
		addr_table_read(addr_table[rand_num[j]],local_buf);
	}
}

void workloadd() 
{
	int i=0;
	int const_read = 0;
	int read_num,update_num;
	read_num = accessNum *0.95;
	update_num = accessNum *0.05;
	for(;i < accessNum ;i++)
	{
		j = i % obj_num;
		srand(i);
		int suiji = rand()%(read_num+update_num);
		{
//		printf("workloadd suiji=%d now=%d\n",suiji,now_num);
			if(suiji < read_num)
			{
				read_num--;
				addr_table_read(addr_table[const_read],local_buf);
		
			}
			else
			{
				update_num --;
				addr_table_insert(addr_table[now_num+1],local_buf);
				const_read++;
			}
		}
		
	}
}

void workloade()
{
	int i=0;int k;
	pick(obj_num);
	int read_num,update_num;
	read_num = accessNum *0.95;
	update_num = accessNum *0.05;
	for(;i < accessNum ;i++)
	{
		j = i % obj_num;
		srand(i);
		int suiji = rand()%(read_num+update_num);
		{
			if(suiji < read_num)
			{
				read_num--;
				int scan_num = rand()%10 + 1;
				for(k = 0;k < scan_num;k++)
				{
					temp[k] = addr_table[(rand_num[j] +k) % obj_num];
				//	printf("temp %d=%p\n",k,temp[k]);
				}
				addr_table_scan(scan_num,local_buf);
			}
			else
			{
				update_num --;
		addr_table_insert(addr_table[now_num+1],local_buf);
				now_num++;
			}
		}
		
	}
}

void workloadf()
{
	int i=0;
	pick(obj_num);
	int read_num,update_num;
	read_num = accessNum /2;
	update_num = accessNum /2;
	for(;i < accessNum ;i++)
	{
		j = i % obj_num;
		srand(i);
		int suiji = rand()%(read_num+update_num);
		{
			if(suiji < read_num)
			{
				read_num--;
				addr_table_read(addr_table[rand_num[j]],local_buf);
		
			}
			else
			{
				update_num --;
				addr_table_read(addr_table[rand_num[j]],local_buf);
				addr_table_update(addr_table[rand_num[j]],local_buf);
			}
		}
		
	}
}

int main(int argc,char *argv[])
{
	struct timespec task_time_start, task_time_end;
    unsigned long task_time_diff_ns;
	now_num = obj_num;
	int i;
	dhmp_client_init(value_size,0,0);
	for(i =0;i< totol_size;i++)
	{
#ifdef LOCAL
		addr_table[i] = malloc(value_size);
#else
		addr_table[i] = dhmp_malloc(value_size,0);
#endif
		meta[i] = malloc(sizeof(uintptr_t));
		local_buf = malloc(key_size);
	}
printf("alloc\n");
	memset(meta,0,sizeof(uintptr_t)*totol_size);
	size_t size= value_size;
//      dhmp_malloc(sizeof(struct parad ), 10);
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
    dhmp_malloc(size, 10);
#endif

    for(i = 0; i < obj_num;i++)
    {
    	addr_table_insert(addr_table[i], local_buf);
    }

    clock_gettime(CLOCK_MONOTONIC, &task_time_start);

int type=atoi(argv[1]);
	printf("type = %d",type);
    if(type == 1)
    	workloada();
    else if(type == 2)
    	workloadb();
    else if(type == 3)
    	workloadc();
    else if(type == 4)
    	workloadd();
    else if(type == 5)
    	workloade();
	else if(type == 6)
    	workloadf();
    clock_gettime(CLOCK_MONOTONIC, &task_time_end);
    task_time_diff_ns = ((task_time_end.tv_sec * 1000000000) + task_time_end.tv_nsec) -
                    ((task_time_start.tv_sec * 1000000000) + task_time_start.tv_nsec);
    printf("runtime %lf\n", (double)task_time_diff_ns/1000000);
    dhmp_client_destroy();
	return;
}
