#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_hash.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "dhmp_work.h"
#include "dhmp_client.h"
#include "dhmp_server.h"
//4KB:4096,8192,16384,32768,65536,131072,262144
//8KB:8192,16384,32768,65536,131072,262144,524288
//16KB:16384,32768,65536,131072,262144,524288
//32KB:32768,65536,131072,262144,524288,1048576
//64KB:65536,131072,262144,524288,1048576,2097152
//128KB:131072,262144,524288,1048576,2097152,4194304
//256KB:262144,524288,1048576,2097152,4194304,8388608
//512KB:524288,1048576,2097152,4194304,8388608,16777216
//1MB:1048576,2097152,4194304,8388608,16777216,33554432
//2MB:2097152,4194304,8388608,16777216,33554432,67108864
//3MB:3145728,6291456,12582912,25165824,50331648,100663296
//4MB:4194304,8388608,16777216,33554432,67108864,134217728
//5MB:5242880,10485760,20971520,41943040,83886080,167772160
//6MB:6291456,12582912,25165824,50331648,100663296,201326592
//8MB:8388608,16777216,33554432,67108864,134217728,268435456
//32MB:33554432,67108864,134217728,268435456,536870912
//64MB:67108864,134217728,268435456,536870912,1073741824,2147483648

struct dhmp_server *server=NULL;


/**
 *	dhmp_get_dev_from_server:get the dev_ptr from dev_list of server.
 */
struct dhmp_device *dhmp_get_dev_from_server()
{
	struct dhmp_device *res_dev_ptr=NULL;
	if(!list_empty(&server->dev_list))
	{
		res_dev_ptr=list_first_entry(&server->dev_list,
									struct dhmp_device,
									dev_entry);
	}
		
	return res_dev_ptr;
}


struct dhmp_area *dhmp_area_create(size_t length)
{
	void *addr=NULL;
	struct dhmp_area *area=NULL;
	struct ibv_mr *mr;
	struct dhmp_device *dev;
	bool res;
	
	/*nvm memory*/
	addr=nvm_malloc(length);
	if(!addr)
	{
		ERROR_LOG("allocate nvm memory error.");
		return NULL;
	}

	dev=dhmp_get_dev_from_server();
	mr=ibv_reg_mr(dev->pd,
				addr, length, 
				IBV_ACCESS_LOCAL_WRITE|
				IBV_ACCESS_REMOTE_READ|
				IBV_ACCESS_REMOTE_WRITE|
				IBV_ACCESS_REMOTE_ATOMIC);
	if(!mr)
	{
		ERROR_LOG("ib register mr error.");
		goto out_addr;
	}

	area=malloc(sizeof(struct dhmp_area));//numa_alloc_onnode(sizeof(struct dhmp_area),3);
	if(!area)
	{
		ERROR_LOG("allocate memory error.");
		goto out_mr;
	}

	area->mr=mr;
	area->size = length;

		list_add(&area->area_entry, &server->area_list[0]);
	
	return area;

out_area:
	free(area);
out_mr:
	ibv_dereg_mr(mr);
out_addr:
	nvm_free(addr, length);
	return NULL;
}


struct ibv_mr * dhmp_malloc_poll_area(size_t length)
{
	void *addr=NULL;
	struct ibv_mr *mr;
	struct dhmp_device *dev;
	bool res;
	
	/*nvm memory*/
	addr = nvm_malloc(length);
	if(!addr)
	{
		ERROR_LOG("allocate nvm memory error.");
		return NULL;
	}

	dev=dhmp_get_dev_from_server();
	mr=ibv_reg_mr(dev->pd,
				addr, length, 
				IBV_ACCESS_LOCAL_WRITE|
				IBV_ACCESS_REMOTE_READ|
				IBV_ACCESS_REMOTE_WRITE|
				IBV_ACCESS_REMOTE_ATOMIC);
	if(!mr)
	{
		ERROR_LOG("ib register mr error.");
		nvm_free(addr, length);
		return NULL;
	}
	// server->ringbufferAddr = addr;
	return mr;
}

void *model_B_write_run()
{
	INFO_LOG("start epoll");
	while(1)
	{
		if(server->ringbufferAddr == NULL) continue;
		if((*((bool *)(server->ringbufferAddr))) == true)
		{
			INFO_LOG("get new model_B message");
			dhmp_Write2_request_handler();
			*((bool *)(server->ringbufferAddr)) = false;
		}
	}
	return NULL;
}

void *model_C_Sread_run()
{
	while(1)
	{
		if(server->model_C_new_msg == false) continue;
		INFO_LOG("get new model_C message");
		dhmp_Sread_server();
		server->model_C_new_msg = false;
	}
	return NULL;
}

void amper_L5_request_handler(int node_id)
{
	void * context = server->L5_message[node_id].addr;
	void * server_addr = (void*)*(uintptr_t *)context;
	size_t size = *(size_t *)(context + sizeof(uintptr_t));
	char write_flag = (char)*(char *)(context + sizeof(uintptr_t) + sizeof(size_t));

	void *temp = server->L5_message[node_id].reply_smr->mr->addr;
	size_t reply_size= 0;
	if(write_flag == 1)
	{
		memcpy(server_addr, context + sizeof(uintptr_t) + sizeof(size_t) + 1, size);
		_mm_clflush(server_addr);
		*(char *)temp = 1;
		temp += 1;
		reply_size = sizeof(size_t) + 1;
	}
	else
	{
		memcpy(temp, server_addr, size);
		temp += size;
		reply_size= sizeof(size_t) + size;
	}
	memcpy(temp, &size, sizeof(size_t));

	struct ibv_send_wr send_wr,*bad_wr=NULL;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	struct ibv_sge sge;
	struct dhmp_task* task;
	task = dhmp_write_task_create(server->connect_trans[node_id], NULL, 0);
	if (!task )
	{
		ERROR_LOG ( "allocate memory error." );
		return ;
	}
	*(uintptr_t *)(server->L5_message[node_id].addr) = 0;
	amper_post_write(task, &(server->L5_message[node_id].C_mr), 
		server->L5_message[node_id].reply_smr->mr->addr, reply_size, server->L5_message[node_id].reply_smr->mr->lkey, false); 
	while(!task->done_flag);
	
	server->L5_message[node_id].is_new = true;
	free(task);
	return;
}

void *L5_run()
{
	INFO_LOG("start L5 epoll");
	int i = 0;
	while(1)
	{
		if(server->L5_mailbox.addr == NULL) continue;
		if((*((char *)(server->L5_mailbox.addr + i))) != 0)
		{
			INFO_LOG("get new L5 message %d",i);
			*((char *)(server->L5_mailbox.addr + i)) = 0;
			amper_L5_request_handler(i);
		}
		i = (i+1) % server->client_num;
	}
	return;
}

void *L5_flush2_run(void* arg1)
{
	int node_id = *(int*) arg1;
	uintptr_t now;
	INFO_LOG("start L5 epoll");
	int i;
	while(1)
	{
		if(server->L5_message[node_id].addr == NULL) continue;
		now = *(uintptr_t *)(server->L5_message[node_id].addr);
		while(server->L5_message[node_id].is_new != true);
		while(now == 0);
		INFO_LOG("get new L5 FLUSH2 message");
		struct dhmp_task* task;
		task = dhmp_write_task_create(server->connect_trans[node_id], NULL, 0);
		if (!task )
		{
			ERROR_LOG ( "allocate memory error." );
			return ;
		}
		char num_1 = 1;
		amper_post_write(task, &(server->L5_message[node_id].C_mr), (uint64_t *)&(num_1), sizeof(char), 0, true); 
		while(!task->done_flag);
		free(task);
		server->L5_message[node_id].is_new = false;
	}
	return;
}

static char check_valid(char *data, size_t size)
{
	int i = 0;
	for(; i<size ;i++)
		if(data[i] != 0)
			return 1;
	return 0;
}

void *tailwind_flush2_run(void* arg1)
{
	int node_id = *(int*) arg1;
	size_t size = *(size_t*) (arg1 + sizeof(int));
	INFO_LOG("start tailwind epoll");
	int i;
	while(1)
	{
		for(i = 1;i < Tailwind_log_size-1;i++)
		{
			void * temp = server->Tailwind_buffer[node_id].addr;
			while( check_valid(temp + i*size , size) == 0);

			INFO_LOG("get new tailwind FLUSH2 message");
			struct dhmp_task* task;
			task = dhmp_write_task_create(server->connect_trans[node_id], NULL, 0);
			if (!task )
			{
				ERROR_LOG ( "allocate memory error." );
				return ;
			}
			char num_1 = 1;
			amper_post_write(task, &(server->Tailwind_buffer[node_id].flush2_mr), (uint64_t*)&(num_1), sizeof(char), 0, true); 
			while(!task->done_flag);
			free(task);
		}
	}
	return;
}



void amper_RFP_request_handler(int node_id , size_t size)
{
	void * context = server->RFP[node_id].write_mr->addr + size;
	char write_flag = (char)*(char *)context;
	void * server_addr = (void*)*(uintptr_t *)(context+1);

	void *temp = server->RFP[node_id].read_mr->addr;
	memcpy(temp + 1, &size, sizeof(size_t));
	size_t reply_size;
	if(write_flag == 1)
	{
		memcpy(server_addr, server->RFP[node_id].write_mr->addr, size);
		_mm_clflush(server_addr);
		temp = temp + sizeof(size_t) + 1;
	}
	else
	{
		memcpy(temp + sizeof(size_t) + 1, server_addr, size);
		temp = temp + sizeof(size_t) + 1 + size;
	}

	if(server->RFP[node_id].time == 1)
	{
		*(char*)temp = 1;
		server->RFP[node_id].time = 2;
	}
	else
	{
		*(char*)temp = 2;
		server->RFP[node_id].time = 1;
	}
	return;
}

void *RFP_run(void* arg1)
{
	int node_id = *(int*) arg1;
	size_t size = server->RFP[node_id].size;
	server->RFP[node_id].time = 1;
	char * poll_addr = server->RFP[node_id].write_mr->addr + size + sizeof(uintptr_t) +sizeof(size_t) +1;
	INFO_LOG("start RFP epoll");
	while(1)
	{
		if(server->RFP[node_id].write_mr == NULL) continue;
		if(*poll_addr != 0)
		{
			INFO_LOG("get new RFP message");
			amper_RFP_request_handler(node_id,size);
			*poll_addr = 0;
			
		}
		if(server->RFP[node_id].time == 0)
			break;
	}
	return;
}




void *FaRM_run(void* arg1)
{
	int node_id = *(int*) arg1;
	int i = 0;
	INFO_LOG("start FaRM_run epoll");
	char * reply, * valid;
	char write_flag;
	void* local_addr,*server_addr;
	size_t size =  server->FaRM[node_id].size;
	size_t buffer_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2 + size;
	void* temp = malloc(buffer_size);
	server->FaRM[node_id].local_mr = dhmp_create_smr_per_ops(server->connect_trans[node_id], temp, buffer_size)->mr;

	reply = server->FaRM[node_id].S_mr->addr + size;
	valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
	while(1)
	{
		if(*valid == 0)
			continue;
		*valid = 0;
		INFO_LOG("get new FaRM messge");
		server_addr = (void*)*(uintptr_t*)reply;
		local_addr = (void*)*(uintptr_t*)(reply + sizeof(uintptr_t));
		write_flag = *(reply + sizeof(uintptr_t)*2 + sizeof(size_t));

		size_t reply_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2;
		if(write_flag == 0)
			reply_size += size;
		
		temp = server->FaRM[node_id].local_mr->addr;
		
		if(write_flag == 1)
		{
			memcpy(server_addr, server->FaRM[node_id].S_mr->addr + i * buffer_size, size);
			_mm_clflush(server_addr);
		}
		else
		{
			memcpy(temp, server_addr, size);
			temp += size;
		}
		
		*(uintptr_t*) temp = (uintptr_t)server_addr;
		*(uintptr_t*)(temp + sizeof(uintptr_t)) = (uintptr_t)local_addr;
		*(size_t*)(temp + sizeof(uintptr_t)*2) = size;
		*(char*)(temp + sizeof(uintptr_t)*2 + sizeof(size_t)) = write_flag;
		*(char*)(temp + sizeof(uintptr_t)*2 + sizeof(size_t) + 1) = 1; //valid

		struct dhmp_task* task;
		task = dhmp_write_task_create(server->connect_trans[node_id], NULL, 0);
		if (!task )
		{
			ERROR_LOG ( "allocate memory error." );
			return ;
		}
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		struct ibv_sge sge;
		struct dhmp_send_mr* temp_mr=NULL;
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.wr_id= ( uintptr_t ) task;
		send_wr.opcode=IBV_WR_RDMA_WRITE;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
		send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr= ( uintptr_t ) server->FaRM[node_id].C_mr.addr + (i * buffer_size);
		if(write_flag == 1)
			send_wr.wr.rdma.remote_addr += size;
		send_wr.wr.rdma.rkey= server->FaRM[node_id].C_mr.rkey;
		sge.addr= ( uintptr_t )server->FaRM[node_id].local_mr->addr;
		sge.length= reply_size;
		sge.lkey= server->FaRM[node_id].local_mr->lkey;
		int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
		if ( err )
		{
			ERROR_LOG("ibv_post_send error");
			exit(-1);
			return ;
		}	
		while(!task->done_flag);
		free(task);

		i = (i + 1) % FaRM_buffer_NUM;
		reply = server->FaRM[node_id].S_mr->addr + size + (i * buffer_size);
		valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
		if(server->FaRM[node_id].size == 0)
		{
			ibv_dereg_mr(server->FaRM[node_id].local_mr);
			break;
		}
	}

	return;
}


void *herd_run(void* arg1)
{
	int node_id = *(int*) arg1;
	int i = 0;
	INFO_LOG("start herd_run epoll");
	char * reply, * valid;
	char write_flag;
	void* local_addr,*server_addr;
	size_t size =  server->herd[node_id].size;
	size_t buffer_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2 + size;
	reply = server->herd[node_id].S_mr->addr + size;
	valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
	while(1)
	{
		if(*valid == 0)
			continue;
		*valid = 0;
		INFO_LOG("get new herd messge");
		server_addr = (void*)*(uintptr_t*)reply;
		local_addr = (void*)*(uintptr_t*)(reply + sizeof(uintptr_t));
		write_flag = *(reply + sizeof(uintptr_t)*2 + sizeof(size_t));

		size_t reply_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2;
		if(write_flag == 0)
			reply_size += size;
		void *respond = malloc(reply_size);
		void * temp = respond;
		*(char*)(temp) = write_flag;
		*(char *)(temp + 1) = i;
		*(uintptr_t*)(temp + 2) = (uintptr_t)server_addr;
		*(uintptr_t*)(temp + 2 + sizeof(uintptr_t)) = (uintptr_t)local_addr;
		*(size_t*)(temp + 2 + sizeof(uintptr_t)*2) = size;
		temp += (2+ sizeof(uintptr_t)*2 + sizeof(size_t));
		if(write_flag == 1)
		{
			memcpy(server_addr, server->herd[node_id].S_mr->addr + i * buffer_size, size);
			_mm_clflush(server_addr);
		}
		else
		{
			memcpy(temp, server_addr, size);
		}

		struct dhmp_msg res_msg;
		res_msg.msg_type=DHMP_MSG_herd_RESPONSE;
		res_msg.data_size= reply_size;
		res_msg.data= respond;
		// amper_ud_post_send(server->herd[node_id].UD_rdma_trans, &res_msg); 

		// while(!task->done_flag);
		// free(task);
		// free(respond);

		i = (i + 1) % FaRM_buffer_NUM;
		reply = server->herd[node_id].S_mr->addr + size + (i * buffer_size);
		valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);


	}

	return;
}

void *pRDMA_run(void* arg1)
{
	int node_id = *(int*) arg1;
	int i = 0;
	INFO_LOG("start pRDMA_run epoll");
	char * reply, * valid;
	char write_flag;
	void* local_addr,*server_addr;
	size_t size =  server->pRDMA[node_id].size;
	size_t buffer_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2 + size;
	reply = server->pRDMA[node_id].S_mr->addr + size;
	valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
	while(1)
	{
		if(server->pRDMA[node_id].size == 0)
		{
			break;
		}
		if(*valid == 0)
			continue;
		*valid = 0;
		INFO_LOG("get new pRDMA messge");
		server_addr = (void*)*(uintptr_t*)reply;
		local_addr = (void*)*(uintptr_t*)(reply + sizeof(uintptr_t));
		write_flag = *(reply + sizeof(uintptr_t)*2 + sizeof(size_t));
		INFO_LOG("get new pRDMA messge %p %p %d",server_addr,local_addr,write_flag);
		size_t reply_size = sizeof(uintptr_t)*2 + sizeof(size_t) + 2;
		if(write_flag == 0)
			reply_size += size;          
		void *respond = malloc(reply_size);
		void * temp = respond;
		*(char*)(temp) = write_flag;
		*(char *)(temp + 1) = i;
		*(uintptr_t*)(temp + 2) = (uintptr_t)server_addr;
		*(uintptr_t*)(temp + 2 + sizeof(uintptr_t)) = (uintptr_t)local_addr;
		*(size_t*)(temp + 2 + sizeof(uintptr_t)*2) = size;
		temp += (2+ sizeof(uintptr_t)*2 + sizeof(size_t));
		if(write_flag == 1)
		{
			memcpy(server_addr, server->pRDMA[node_id].S_mr->addr + i * buffer_size, size);
			_mm_clflush(server_addr);
		}
		else
			memcpy(temp, server_addr, size);

		struct dhmp_msg res_msg;
		res_msg.msg_type=DHMP_MSG_pRDMA_WS_RESPONSE;
		res_msg.data_size= reply_size;
		res_msg.data= respond;
		struct dhmp_task * task = dhmp_post_send(server->connect_trans[node_id], &res_msg); 
		while(!task->done_flag);
		free(task);
		free(respond);

		i = (i + 1) % FaRM_buffer_NUM;
		reply = server->pRDMA[node_id].S_mr->addr + size + (i * buffer_size);
		valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
	}

	return;
}


void amper_scalable_request_handler(int node_id, char batch, char write_type, char write_flag, size_t size)
{ 
	size_t head_size = batch * (sizeof(uintptr_t)*2 + sizeof(size_t) +size);
	size_t offset  = 0;
	int i;
	void * temp,*temp2, *server_addr,*client_addr;
	char * addr = server->scaleRPC[node_id].Sreq_mr->addr;
	temp2 = server->scaleRPC[node_id].Sdata_mr->addr;
	if(write_flag == 1)
	{
		temp2 += (batch * size);
		offset += (batch * size);
	}
	else
	{
		addr += (batch * size);
	}
	if(write_type == 1)
	{
		struct dhmp_task* scalable_task;
		scalable_task = dhmp_read_task_create(server->connect_trans[node_id], NULL, 0);
		if(!scalable_task)
		{
			ERROR_LOG("allocate memory error.");
			return;
		}
		size_t read_size = *(size_t*)(addr+head_size - sizeof(size_t) - sizeof(struct ibv_mr));
		INFO_LOG("read_size = %d",read_size);
		struct ibv_mr * temp_mr = (struct ibv_mr*)(addr+head_size - sizeof(struct ibv_mr));
		INFO_LOG("read_sover = %p",temp_mr->addr );
		amper_post_read(scalable_task, temp_mr, 
				server->scaleRPC[node_id].Slocal_mr->addr, read_size, server->scaleRPC[node_id].Slocal_mr->lkey, false);
		while(!scalable_task->done_flag);
		free(scalable_task);
		INFO_LOG("read_sover = ");
		temp = server->scaleRPC[node_id].Slocal_mr->addr;
		
		for(i =0;i<batch;i++)
		{
			server_addr = (void*)*(uintptr_t *)temp;
			client_addr = (void*)*(uintptr_t *)(temp + sizeof(uintptr_t));
			temp = temp + sizeof(uintptr_t)*2 + sizeof(size_t);
			if(write_flag == 1)  {memcpy(server_addr, temp, size); temp += size;_mm_clflush(server_addr);}
			
			*(uintptr_t*)temp2 = (uintptr_t)server_addr;
			*(uintptr_t*)(temp2 + sizeof(uintptr_t)) = (uintptr_t)client_addr;
			*(size_t*)(temp2+sizeof(uintptr_t)*2) = size;
			temp2 = temp2 + sizeof(uintptr_t)*2 + sizeof(size_t);
			if(write_flag == 0)  {memcpy(temp2, server_addr, size); temp2 += size;}
		}
	}
	else
	{
		for(i =0;i<batch;i++)
		{
			server_addr = (void*)*(uintptr_t *)addr;
			client_addr = (void*)*(uintptr_t *)(addr + sizeof(uintptr_t));
			addr = addr + sizeof(uintptr_t)*2 + sizeof(size_t);
			if(write_flag == 1)  {memcpy(server_addr, addr, size); addr += size;_mm_clflush(server_addr);}
			*(uintptr_t*)temp2 = (uintptr_t)server_addr;
			*(uintptr_t*)(temp2 + sizeof(uintptr_t)) = (uintptr_t)client_addr;
			*(size_t*)(temp2+sizeof(uintptr_t)*2) = size;
			temp2 = temp2 + sizeof(uintptr_t)*2 + sizeof(size_t);
			if(write_flag == 0)  {memcpy(temp2, server_addr, size); temp2 += size;}
		}
	}
	size_t write_size;		
	write_size = 1 + batch * (sizeof(uintptr_t)*2 + sizeof(size_t));	
	if(write_flag == 0)
		write_size += (batch * size);	
	*(char*)temp2 = 1; //valid

	struct dhmp_task* scalable_task2;
	scalable_task2 = dhmp_write_task_create(server->connect_trans[node_id], NULL, 0);
	if(!scalable_task2)
	{
		ERROR_LOG("allocate memory error.");
		return;
	}
	INFO_LOG("%d Cdata= %p totolsize=%d %d dhmp=%p clinet=%p",node_id,server->scaleRPC[node_id].Cdata_mr.addr,write_size,offset, server_addr,client_addr);
	{
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		struct ibv_sge sge;
		struct dhmp_send_mr* temp_mr=NULL;
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.wr_id= ( uintptr_t ) scalable_task2;
		send_wr.opcode=IBV_WR_RDMA_WRITE;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
			send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr= ( uintptr_t )server->scaleRPC[node_id].Cdata_mr.addr + offset;
		send_wr.wr.rdma.rkey= server->scaleRPC[node_id].Cdata_mr.rkey;
		sge.addr= ( uintptr_t ) server->scaleRPC[node_id].Sdata_mr->addr + offset;
		sge.length= write_size;
		sge.lkey= server->scaleRPC[node_id].Sdata_mr->lkey;
		int err=ibv_post_send ( scalable_task2->rdma_trans->qp, &send_wr, &bad_wr );
		if ( err )
		{
			ERROR_LOG("ibv_post_send error");
			exit(-1);
			return;
		}	
	}
	while(!scalable_task2->done_flag);

	return;
}

void *scalable_run(void* arg1)
{
	char batch = BATCH;
	int node_id = *(int*) arg1;
	size_t size = server->scaleRPC[node_id].size;
	INFO_LOG("start scaleRPC %d epoll", node_id);
	char * addr,*valid;
	size_t head_size = batch * (sizeof(uintptr_t)*2 + sizeof(size_t) +size);
	int i = 0;
	while(1)
	{
		if(*(char *)(server->scaleRPC[node_id].Sreq_mr->addr + head_size +3))
		{
			addr = server->scaleRPC[node_id].Sreq_mr->addr + head_size;
			INFO_LOG("get new scaleRPC type = %d message %d %d ", addr[0],addr[1],addr[2]);
			amper_scalable_request_handler(node_id, batch, *(addr), *(addr+2), size); 
			addr[3] = 0;
			INFO_LOG("start scaleRPC %p epoll", server->scaleRPC[node_id].Sreq_mr->addr);
		}
		if(server->scaleRPC[node_id].Slocal_mr == NULL)
			break;
	}
	return ;
}


void dhmp_server_init()
{
	int i,err=0;
	
	server=(struct dhmp_server *)malloc(sizeof(struct dhmp_server));
	if(!server)
	{
		ERROR_LOG("allocate memory error.");
		return ;
	}
	
	dhmp_hash_init();
	dhmp_config_init(&server->config, false);
	dhmp_context_init(&server->ctx);

	/*init client transport list*/
	server->cur_connections=0;
	pthread_mutex_init(&server->mutex_client_list, NULL);
	INIT_LIST_HEAD(&server->client_list);
	
	/*init list about rdma device*/
	INIT_LIST_HEAD(&server->dev_list);
	dhmp_dev_list_init(&server->dev_list);

	/*init the structure about memory count*/
	/*get dram total size, get nvm total size*/
	server->nvm_total_size=numa_node_size(1, NULL);

	server->ringbufferAddr = NULL;
	server->cur_addr = 0;
	server->nvm_used_size=0;
	INFO_LOG("server nvm total size %ld",server->nvm_total_size);

	server->listen_trans=dhmp_transport_create(&server->ctx,
											dhmp_get_dev_from_server(),
											true, false);
	if(!server->listen_trans)
	{
		ERROR_LOG("create rdma transport error.");
		exit(-1);
	}

	//##############initial
	server->client_num = 0;
	

#ifdef DaRPC_SERVER
	for(i=0; i<DaRPC_clust_NUM; i++)
	{
		server->DaRPC_dcq_count[i] = 0;
		server->DaRPC_dcq[i] = NULL;
	}	
#endif
	server->L5_mailbox.addr = NULL;
	for(i=0; i<DHMP_CLIENT_NODE_NUM; i++)
	{
		server->Tailwind_buffer[i].addr = NULL;
	}

#ifdef UD
	err=dhmp_transport_listen_UD(server->listen_trans,
					server->config.net_infos[server->config.curnet_id].port);
#else
	err=dhmp_transport_listen(server->listen_trans,
					server->config.net_infos[server->config.curnet_id].port);
#endif
	if(err)
		exit(- 1);

	/*create one area and init area list*/	
	INIT_LIST_HEAD(&server->area_list[0]);
	// server->cur_area=dhmp_area_create(2097152);

#ifdef model_B
	pthread_create(&server->model_B_write_epoll_thread, NULL, model_B_write_run, NULL);
#endif
#ifdef model_C
	pthread_create(&server->model_C_Sread_epoll_thread, NULL, model_C_Sread_run, NULL);
#endif

#ifdef L5
	pthread_create(&server->L5_poll_thread, NULL, L5_run, NULL);
#endif	
}

void dhmp_server_destroy()
{
	INFO_LOG("server destroy start.");
#ifdef model_B
	pthread_join(server->model_B_write_epoll_thread, NULL);
#endif
#ifdef model_C
	pthread_join(server->model_C_Sread_epoll_thread, NULL);
#endif	
	pthread_join(server->ctx.epoll_thread, NULL);

	int i;
#ifdef L5
	pthread_join(server->L5_poll_thread, NULL);
#endif	
#ifdef RFP
	for(i =0;i<DHMP_CLIENT_NODE_NUM ;i++)
		pthread_join(server->RFP[i].poll_thread, NULL);
#endif	
#ifdef scaleRPC
	for(i =0;i<DHMP_CLIENT_NODE_NUM ;i++)
		pthread_join(server->scalable_poll_thread[i], NULL);
#endif
	INFO_LOG("server destroy end.");
	free(server);
}


void amper_allocspace_for_server(struct dhmp_transport* rdma_trans, int is_special, size_t size)
{
	int node_id = rdma_trans->node_id;
	struct dhmp_send_mr* temp_smr;
	void * temp;
	switch(is_special)
	{
		case 3: // L5
		{
			if(node_id == 0) //for test
			{
				server->L5_mailbox.addr = nvm_malloc(DHMP_CLIENT_NODE_NUM*sizeof(char)); // store client num  
				temp_smr = dhmp_create_smr_per_ops(rdma_trans, server->L5_mailbox.addr, DHMP_CLIENT_NODE_NUM*sizeof(char));
				server->L5_mailbox.mr = temp_smr->mr;
				// temp = nvm_malloc(sizeof(char)); 
				// temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, sizeof(char));
				// server->L5_mailbox.read_mr = temp_smr->mr;
			}
			//only use server's first dev
			server->L5_message[node_id].addr = nvm_malloc(size+18);
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, server->L5_message[node_id].addr, size+18);
			server->L5_message[node_id].mr = temp_smr->mr;
			INFO_LOG("L5 buffer %d %p is ready.",node_id,server->L5_mailbox.mr->addr);
			void * temp = malloc(size + sizeof(size_t));
			server->L5_message[node_id].reply_smr = dhmp_create_smr_per_ops(rdma_trans, temp , size + sizeof(size_t));
		}
		break;
		case 4: // for tailwind
		{
			if(server->Tailwind_buffer[node_id].addr != NULL)
			{
				ibv_dereg_mr(server->Tailwind_buffer[node_id].mr);
				nvm_free(server->Tailwind_buffer[node_id].addr, size);
			}
			server->Tailwind_buffer[node_id].addr = nvm_malloc(size); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, server->Tailwind_buffer[node_id].addr, size);
			server->Tailwind_buffer[node_id].mr = temp_smr->mr;
			free(temp_smr);//!!!!!!!!!!!!!!!
#ifdef FLUSH2
			struct {int node_id;size_t size;} data;
			data.node_id = node_id;
			data.size = size;
			pthread_create(&server->Tailwind_buffer[node_id].poll_thread, NULL, tailwind_flush2_run, &data);
#endif
		}
		break;
		case 5: // for DaRPC only need once
		{
			temp = nvm_malloc(sizeof(char)); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, sizeof(char));
			server->read_mr = temp_smr->mr;
		}
		break;
		case 6: // for RFP
		{
			temp = nvm_malloc(size + 24); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, size + 24);
			server->RFP[node_id].write_mr = temp_smr->mr;

			temp = nvm_malloc(size + 24); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, size + 24);
			server->RFP[node_id].read_mr = temp_smr->mr;
			server->RFP[node_id].size = size;
			pthread_create(&server->RFP[node_id].poll_thread, NULL, RFP_run, &node_id);
		}
		break;
		case 7: // for scaleRPC
		{
			int batch = BATCH; 
			size_t totol_length = 4 + batch *(sizeof(uintptr_t)*2 + sizeof(size_t) + size); 
			temp = nvm_malloc(totol_length); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length);
			server->scaleRPC[node_id].Sreq_mr = temp_smr->mr;

			temp = nvm_malloc(totol_length); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length);
			server->scaleRPC[node_id].Sdata_mr = temp_smr->mr;

			temp = nvm_malloc(totol_length); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length);
			server->scaleRPC[node_id].Slocal_mr = temp_smr->mr;
			server->scaleRPC[node_id].size = size;
			pthread_create(&(server->scalable_poll_thread[node_id]), NULL, scalable_run, &node_id);
		}
		break;
		case 8: // for FaRM
		{
			size_t totol_length = 2 + sizeof(uintptr_t)*2 + sizeof(size_t) + size; // batch + writeORread + readmr  
			temp = nvm_malloc(totol_length * FaRM_buffer_NUM); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length * FaRM_buffer_NUM);
			server->FaRM[node_id].S_mr = temp_smr->mr;
			server->FaRM[node_id].size = size;
			pthread_create(&(server->FaRM[node_id].poll_thread), NULL, FaRM_run, &node_id);
		}
		break;
		case 9: // for herd
		{
			size_t totol_length = 1 + 1 + sizeof(uintptr_t)*2 + sizeof(size_t) + size; // batch + writeORread + readmr  
			temp = nvm_malloc(totol_length * FaRM_buffer_NUM); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length * FaRM_buffer_NUM);
			server->herd[node_id].S_mr = temp_smr->mr;
			server->herd[node_id].size = size;
			pthread_create(&(server->herd[node_id].poll_thread), NULL, herd_run, &node_id);
		}
		break;
		case 10: // for pRDMA
		{
			size_t totol_length = 1 + 1 + sizeof(uintptr_t)*2 + sizeof(size_t) + size; // batch + writeORread + readmr  
			temp = nvm_malloc(totol_length * FaRM_buffer_NUM); 
			temp_smr = dhmp_create_smr_per_ops(rdma_trans, temp, totol_length * FaRM_buffer_NUM);
			server->pRDMA[node_id].S_mr = temp_smr->mr;
			server->pRDMA[node_id].size = size;
			pthread_create(&(server->pRDMA[node_id].poll_thread), NULL, pRDMA_run, &node_id);
		}
		break;
	};
	return;
}