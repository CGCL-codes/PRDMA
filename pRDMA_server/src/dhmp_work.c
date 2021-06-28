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

struct timespec time_start, time_end;
unsigned long time_diff_ns;

// show(&time_start,0);
// show(&time_end,1);
static void show( struct timespec* time, char output)
{
	clock_gettime(CLOCK_MONOTONIC, time);	
	if(output == 1)
	{
		time_diff_ns = ((time_end.tv_sec * 1000000000) + time_end.tv_nsec) -
                        ((time_start.tv_sec * 1000000000) + time_start.tv_nsec);
  		fprintf(stderr,"runtime %lf ms\n", (double)time_diff_ns/1000000);
  		fflush(stderr);
	}
}

int amper_post_write(struct dhmp_task* task, struct ibv_mr* rmr, uint64_t* sge_addr, uint32_t sge_length, uint32_t sge_lkey ,bool is_inline)
{
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	if(is_inline)
		send_wr.send_flags=IBV_SEND_SIGNALED | IBV_SEND_INLINE;
	else
		send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) rmr->addr;
	send_wr.wr.rdma.rkey= rmr->rkey;
	sge.addr= ( uintptr_t ) sge_addr;
	sge.length=sge_length;
	sge.lkey=sge_lkey;
	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	
}

int amper_post_read(struct dhmp_task* task, struct ibv_mr* rmr, uint64_t* sge_addr, uint32_t sge_length,uint32_t sge_lkey ,bool is_inline)
{
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_READ;
	send_wr.sg_list=&sge;
	if(sge_length == 0)
		send_wr.num_sge=0;
	else
		send_wr.num_sge=1;
	if(is_inline)
		send_wr.send_flags=IBV_SEND_SIGNALED | IBV_SEND_INLINE;
	else
		send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) rmr->addr;
	send_wr.wr.rdma.rkey= rmr->rkey;
	sge.addr= ( uintptr_t ) sge_addr;
	sge.length=sge_length;
	sge.lkey=sge_lkey;
	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	
}


/**
 * dhmp_hash_in_client:cal the addr into hash key
 */
int dhmp_hash_in_client(void *addr)
{
	uint32_t key;
	int index;

	key=hash(&addr, sizeof(void*));
	/*key is a uint32_t value,through below fomula transfer*/
	index=((key%DHMP_CLIENT_HT_SIZE)+DHMP_CLIENT_HT_SIZE)%DHMP_CLIENT_HT_SIZE;

	return index;
}

/**
 *	dhmp_addr_info_insert_ht:
 *	add new addr info into the addr_info_hashtable in client
 */
static void dhmp_addr_info_insert_ht(void *dhmp_addr,
											struct dhmp_addr_info *addr_info)
{
	int index;
	index=dhmp_hash_in_client(dhmp_addr);
	DEBUG_LOG("insert ht %d %p",index,addr_info->nvm_mr.addr);
	hlist_add_head(&addr_info->addr_entry, &client->addr_info_ht[index]);
}

void dhmp_malloc_work_handler(struct dhmp_work *work)
{
	struct dhmp_malloc_work *malloc_work;
	struct dhmp_msg msg;
	struct dhmp_mc_request req_msg;
	void *res_addr=NULL;

	malloc_work=(struct dhmp_malloc_work*)work->work_data;
	
	/*build malloc request msg*/
	req_msg.req_size=malloc_work->length;
	req_msg.addr_info=malloc_work->addr_info;
	req_msg.is_special=malloc_work->is_special;
	req_msg.task = malloc_work;

	if(malloc_work->is_special == 3)  // for L5
	{
		memcpy(&(req_msg.mr) , client->local_mr, sizeof(struct ibv_mr));
	}
	if(malloc_work->is_special == 4)  // for Tailwind
	{
		memcpy(&(req_msg.mr) , client->local_mr, sizeof(struct ibv_mr));
	}
	if(malloc_work->is_special == 7)  // for scalale
	{
		memcpy(&(req_msg.mr) , client->scaleRPC.Cdata_mr, sizeof(struct ibv_mr)); 
	}
	if(malloc_work->is_special == 8)  // for FaRN
	{
		memcpy(&(req_msg.mr), client->FaRM.C_mr, sizeof(struct ibv_mr)); 
	}	

	msg.msg_type=DHMP_MSG_MALLOC_REQUEST;
	msg.data_size=sizeof(struct dhmp_mc_request);
	msg.data=&req_msg;
#ifdef UD
	amper_ud_post_send(malloc_work->rdma_trans, &msg); 
#else
	dhmp_post_send(malloc_work->rdma_trans, &msg);
#endif
	/*wait for the server return result*/
	while(malloc_work->recv_flag==false);

	if(malloc_work->is_special)
		goto end;	
	
	res_addr=malloc_work->addr_info->nvm_mr.addr;
	
	DEBUG_LOG ("get malloc addr %p", res_addr);
	
	if(res_addr==NULL)
		free(malloc_work->addr_info);
	else
	{
		res_addr= malloc_work->addr_info->nvm_mr.addr;
		malloc_work->addr_info->write_flag=false;
		dhmp_addr_info_insert_ht(res_addr, malloc_work->addr_info);
	}
	
	malloc_work->res_addr=res_addr;
end:
	malloc_work->done_flag=true;
}


/**
 *	dhmp_get_addr_info_from_ht:according to addr, find corresponding addr info
 */
struct dhmp_addr_info *dhmp_get_addr_info_from_ht(int index, void *dhmp_addr)
{
	struct dhmp_addr_info *addr_info;
	
	if(hlist_empty(&client->addr_info_ht[index]))
		goto out;
	else
	{
		hlist_for_each_entry(addr_info, &client->addr_info_ht[index], addr_entry)
		{
			if(addr_info->nvm_mr.addr==dhmp_addr)
				break;
		}
	}
	
	if(!addr_info)
		goto out;
	
	return addr_info;
out:
	return NULL;
}


void dhmp_free_work_handler(struct dhmp_work *work)
{
	struct dhmp_msg msg;
	struct dhmp_addr_info *addr_info;
	struct dhmp_free_request req_msg;
	struct dhmp_free_work *free_work;
	int index;

	free_work=(struct dhmp_free_work*)work->work_data;

	/*get nvm mr from client hash table*/
	index=dhmp_hash_in_client(free_work->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, free_work->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("free addr error.");
		goto out;
	}
	hlist_del(&addr_info->addr_entry);
	
	/*build a msg of free addr*/
	req_msg.addr_info=addr_info;
	memcpy(&req_msg.mr, &addr_info->nvm_mr, sizeof(struct ibv_mr));
	
	msg.msg_type=DHMP_MSG_FREE_REQUEST;
	msg.data_size=sizeof(struct dhmp_free_request);
	msg.data=&req_msg;
	
	DEBUG_LOG("free addr is %p length is %d",
		addr_info->nvm_mr.addr, addr_info->nvm_mr.length);
#ifdef UD
	amper_ud_post_send(free_work->rdma_trans, &msg); 
#else
	dhmp_post_send(free_work->rdma_trans, &msg);
#endif
	

	/*wait for the server return result*/
	while(addr_info->nvm_mr.addr!=NULL);

	free(addr_info);
out:
	free_work->done_flag=true;
}

void dhmp_read_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct dhmp_rw_work *rwork;
	int index;
	struct timespec ts1,ts2;
	long long sleeptime;
	
	rwork=(struct dhmp_rw_work *)work->work_data;
	
	index=dhmp_hash_in_client(rwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, rwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("read addr is error.");
		goto out;
	}

	while(addr_info->write_flag);
	
	++addr_info->read_cnt;
		dhmp_rdma_read(rwork->rdma_trans, &addr_info->nvm_mr,
					rwork->local_addr, rwork->length);

out:
	rwork->done_flag=true;
}
	
void dhmp_write_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct dhmp_rw_work *wwork;
	int index;
	struct timespec ts1,ts2;
	long long sleeptime;
	
	wwork=(struct dhmp_rw_work *)work->work_data;
	
	index=dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("write addr is error.");
		goto out;
	}

	/*check no the same addr write task in qp*/
	while(addr_info->write_flag);
	addr_info->write_flag=true;
	
	++addr_info->write_cnt;
	dhmp_rdma_write(wwork->rdma_trans, addr_info, &addr_info->nvm_mr,
					wwork->local_addr, wwork->length);

out:
	wwork->done_flag=true;
}

int amper_clover_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct amper_clover_work *wwork;
	int index;
	
	wwork=(struct amper_clover_work *)work->work_data;
	
	index=dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("write addr is error.");
		goto out;
	}

	/*check no the same addr write task in qp*/
	// while(addr_info->write_flag);
	// addr_info->write_flag=true;
	// ++addr_info->write_cnt;

	struct dhmp_task* task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	int err=0;
	struct ibv_mr* mr = &(addr_info->nvm_mr);

	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_ATOMIC_CMP_AND_SWP;
	send_wr.sg_list= &sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.atomic.remote_addr= ( uintptr_t ) mr->addr + wwork->length;
	send_wr.wr.atomic.rkey=mr->rkey;
	if(wwork->value)
	{
		send_wr.wr.atomic.compare_add = 0ULL; // if == null; 
		send_wr.wr.atomic.swap = (uint64_t)wwork->value;
	}
	else
	{
		send_wr.wr.atomic.compare_add = 0ULL;//to be 1UUL
		send_wr.wr.atomic.swap = 0ULL;
	}
	sge.addr= (uintptr_t)client->cas_mr->mr->addr;
	sge.length= 8; 
	sge.lkey= client->cas_mr->mr->lkey;

	err=ibv_post_send ( wwork->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	
	while(!task->done_flag);
out:
	wwork->done_flag=true;
}


int amper_scalable_work_handler(struct dhmp_work *work) 
{
	struct amper_scalable_work *wwork = (struct amper_scalable_work *)work->work_data;
	int i;

	struct dhmp_task* scalable_task;
	struct dhmp_task* scalable_task2;
	// client->per_ops_mr = Sreq_mr ;client->per_ops_mr2 = Sdata_mr  

	scalable_task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!scalable_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	{
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		struct ibv_sge sge;
		struct dhmp_send_mr* temp_mr=NULL;
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.wr_id= ( uintptr_t ) scalable_task;
		send_wr.opcode=IBV_WR_RDMA_WRITE;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
			send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr= ( uintptr_t )client->scaleRPC.Sreq_mr.addr + wwork->offset;
		send_wr.wr.rdma.rkey= client->scaleRPC.Sreq_mr.rkey;
		sge.addr= ( uintptr_t ) client->scaleRPC.Creq_mr->addr + wwork->offset;
		sge.length= wwork->length;
		sge.lkey= client->scaleRPC.Creq_mr->lkey;
		int err=ibv_post_send ( scalable_task->rdma_trans->qp, &send_wr, &bad_wr );
		if ( err )
		{
			ERROR_LOG("ibv_post_send error");
			exit(-1);
			return -1;
		}	
	}
	while(!scalable_task->done_flag);
	free(scalable_task);
	INFO_LOG("********scale wite over Sreq=%p Cdata_mr= %p offset=%d",client->scaleRPC.Sreq_mr.addr,client->scaleRPC.Cdata_mr->addr,wwork->offset);

	void * temp;
	size_t head_size = wwork->batch * (sizeof(uintptr_t)*2 + sizeof(size_t) );

	char *valid = client->scaleRPC.Cdata_mr->addr + head_size + wwork->batch * wwork->size;
	while(*valid== 0);
	if(wwork->flag_write == 0)
	{
		temp = client->scaleRPC.Cdata_mr->addr;
		size_t size;
		void *client_addr;
		for(i =0 ;i<wwork->batch;i++)
		{
			client_addr = (void *)*(uintptr_t*)(temp + sizeof(uintptr_t));
			size = *(size_t*)(temp + sizeof(uintptr_t)*2);
			temp = temp + sizeof(uintptr_t)*2 + sizeof(size_t);
			memcpy(client_addr, temp , size);
			temp += size;
		}
	}
	*valid = 0;
out:
	wwork->done_flag=true;
}


int amper_L5_work_handler(struct dhmp_work *work)   /// same as two write
{
	struct amper_L5_work *wwork;
	wwork=(struct amper_L5_work *)work->work_data;
	
	size_t head_size = sizeof(uintptr_t) + sizeof(size_t)+1;
	void *temp_head = client->L5.local_smr1->mr->addr;
	memcpy(temp_head, &(wwork->dhmp_addr) ,sizeof(uintptr_t));
	memcpy(temp_head + sizeof(uintptr_t), &(wwork->length), sizeof(size_t));
	memcpy(temp_head + sizeof(uintptr_t) + sizeof(size_t), &(wwork->flag_write), 1);
	
	if(wwork->flag_write == 1)
	{
		temp_head += (sizeof(uintptr_t) + sizeof(size_t)+1);
		head_size += wwork->length;
		memcpy( temp_head, wwork->local_addr, wwork->length);
		// local_smr2 = dhmp_create_smr_per_ops(wwork->rdma_trans, wwork->local_addr, wwork->length);
	}

	struct dhmp_task* task;
	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge *sge = malloc(sizeof(struct ibv_sge) * 2);
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->L5.message_mr)->addr;
	send_wr.wr.rdma.rkey= (&client->L5.message_mr)->rkey;
	sge[0].addr= ( uintptr_t ) client->L5.local_smr1->mr->addr;
	sge[0].length= head_size;
	sge[0].lkey= client->L5.local_smr1->mr->lkey;
	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	
#ifdef FLUSH1
	struct dhmp_task* L5_task3;
	L5_task3 = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!L5_task3)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	amper_post_read(L5_task3, &client->L5.message_mr, NULL, 0, 0, false);

	while(!L5_task3->done_flag); // write after read
	free(L5_task3);
#endif
	
	while(!task->done_flag);	
	free(task);

	struct dhmp_task* L5_task2;
	L5_task2=dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if ( !L5_task2 )
	{
		ERROR_LOG ( "allocate memory error." );
		return -1;
	}
	amper_post_write(L5_task2, &client->L5.mailbox_mr, &(client->L5.num_1), sizeof(char), 0, true); 
#ifdef FLUSH1
	struct dhmp_task* L5_task4;
	L5_task4 = dhmp_read_task_create(wwork->rdma_trans, NULL, 0);
	if(!L5_task4)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	amper_post_read(L5_task4, &client->L5.mailbox_mr, NULL, 0, 0, false);
	while(!L5_task4->done_flag);
	free(L5_task4);
#endif

	while(!L5_task2->done_flag);
	free(L5_task2);

	char * reply = client->local_mr->addr;
	if(wwork->flag_write)
		reply += 1;
	else
		reply += wwork->length;
	while(*(size_t*)reply == 0);
	if(wwork->flag_write == 1)
	{
		INFO_LOG("L5 reply %d",(int)*(char *)(client->local_mr->addr));
	}
	else
		memcpy(wwork->local_addr , client->local_mr->addr, *reply);
	*(size_t*)reply = 0;
	wwork->done_flag=true;
}

//old one
// int amper_L5_work_handler(struct dhmp_work *work)   /// same as two write
// {
// 	struct amper_L5_work *wwork;
// 	wwork=(struct amper_L5_work *)work->work_data;
// 	struct dhmp_send_mr* local_smr2 = NULL;
	
// 	size_t head_size = sizeof(uintptr_t) + sizeof(size_t)+1;
// 	void *temp_head = client->L5.local_smr1->mr->addr;
// 	memcpy(temp_head, &(wwork->dhmp_addr) ,sizeof(uintptr_t));
// 	memcpy(temp_head + sizeof(uintptr_t), &(wwork->length), sizeof(size_t));
// 	memcpy(temp_head + sizeof(uintptr_t) + sizeof(size_t), &(wwork->flag_write), 1);
	
// 	if(wwork->flag_write == 1)
// 		local_smr2 = dhmp_create_smr_per_ops(wwork->rdma_trans, wwork->local_addr, wwork->length);

// 	struct dhmp_task* task;
// 	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
// 	if(!task)
// 	{
// 		ERROR_LOG("allocate memory error.");
// 		return -1;
// 	}
// 	struct ibv_send_wr send_wr,*bad_wr=NULL;
// 	struct ibv_sge *sge = malloc(sizeof(struct ibv_sge) * 2);
// 	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
// 	send_wr.wr_id= ( uintptr_t ) task;
// 	send_wr.opcode=IBV_WR_RDMA_WRITE;
// 	send_wr.sg_list=sge;
// 	if(wwork->flag_write == 1)
// 		send_wr.num_sge=2;
// 	else
// 		send_wr.num_sge=1;
// 	send_wr.send_flags=IBV_SEND_SIGNALED;
// 	send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->L5.message_mr)->addr;
// 	send_wr.wr.rdma.rkey= (&client->L5.message_mr)->rkey;
// 	sge[0].addr= ( uintptr_t ) client->L5.local_smr1->mr->addr;
// 	sge[0].length= head_size;
// 	sge[0].lkey= client->L5.local_smr1->mr->lkey;
// 	if(wwork->flag_write == 1)
// 	{
// 		sge[1].addr= ( uintptr_t ) local_smr2->mr->addr;
// 		sge[1].length= wwork->length;
// 		sge[1].lkey= local_smr2->mr->lkey;
// 	}
// 	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
// 	if ( err )
// 	{
// 		ERROR_LOG("ibv_post_send error");
// 		exit(-1);
// 		return -1;
// 	}	
// #ifdef FLUSH1
// 	struct dhmp_task* L5_task3;
// 	L5_task3 = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
// 	if(!L5_task3)
// 	{
// 		ERROR_LOG("allocate memory error.");
// 		return -1;
// 	}
// 	amper_post_read(L5_task3, &client->L5.message_mr, NULL, 0, 0, false);

// 	while(!L5_task3->done_flag); // write after read
// 	free(L5_task3);
// #endif
	
// #ifdef FLUSH2
// 	char * flush2_tag = client->local_mr->addr;
// 	while(*flush2_tag == 0);
// 	*flush2_tag = 0;
// #endif
// 	while(!task->done_flag);	
// 	free(task);

// 	struct dhmp_task* L5_task2;
// 	L5_task2=dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
// 	if ( !L5_task2 )
// 	{
// 		ERROR_LOG ( "allocate memory error." );
// 		return -1;
// 	}
// 	amper_post_write(L5_task2, &client->L5.mailbox_mr, &(client->L5.num_1), sizeof(char), 0, true); 
// #ifdef FLUSH1
// 	struct dhmp_task* L5_task4;
// 	L5_task4 = dhmp_read_task_create(wwork->rdma_trans, NULL, 0);
// 	if(!L5_task4)
// 	{
// 		ERROR_LOG("allocate memory error.");
// 		return -1;
// 	}
// 	amper_post_read(L5_task4, &client->L5.mailbox_mr, NULL, 0, 0, false);
// 	while(!L5_task4->done_flag);
// 	free(L5_task4);
// #endif

	
// 	while(!L5_task2->done_flag);
// 	free(L5_task2);

// 	char * reply = client->local_mr->addr;
// 	if(wwork->flag_write)
// 		reply += 1;
// 	else
// 		reply += wwork->length;
// 	while(*(size_t*)reply == 0);
// 	if(wwork->flag_write == 1)
// 	{
// 		INFO_LOG("L5 reply %d",(int)*(char *)(client->local_mr->addr));
// 	}
// 	else
// 		memcpy(wwork->local_addr , client->local_mr->addr, *reply);
// 	if(local_smr2 != NULL)
// 	{
// 		ibv_dereg_mr(local_smr2->mr);
// 		free(local_smr2);
// 	}
// 	*(size_t*)reply = 0;
// 	wwork->done_flag=true;
// }

int amper_herd_work_handler(struct dhmp_work *work)   
{
	// struct amper_L5_work *wwork;
	// wwork=(struct amper_L5_work *)work->work_data;
	// struct dhmp_send_mr* local_smr = NULL ;
	
	// size_t head_size = sizeof(uintptr_t)*2 + sizeof(size_t)+2;
	// size_t buffer_size = head_size + wwork->length;
	// void *temp_head ,*temp_all;

	// if(wwork->flag_write == 1)
	// 	head_size += wwork->length;
	// temp_all = malloc(head_size);
	// temp_head = temp_all;
	// if(wwork->flag_write == 1)
	// {
	// 	memcpy(temp_all, wwork->local_addr , wwork->length);
	// 	temp_head += wwork->length;
	// }
	// memcpy(temp_head, &(wwork->dhmp_addr) ,sizeof(uintptr_t));
	// memcpy(temp_head + sizeof(uintptr_t), &(wwork->local_addr) ,sizeof(uintptr_t));
	// memcpy(temp_head + sizeof(uintptr_t)*2, &(wwork->length), sizeof(size_t));
	// memcpy(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t), &(wwork->flag_write), 1);
	// *(char*)(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t)+1) = 1;//valid
	
	// local_smr = dhmp_create_smr_per_ops(wwork->rdma_trans, temp_all, head_size);

	// struct dhmp_task* task;
	// task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	// if(!task)
	// {
	// 	ERROR_LOG("allocate memory error.");
	// 	return -1;
	// }
	// struct ibv_send_wr send_wr,*bad_wr=NULL;
	// struct ibv_sge sge;
	// memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	// send_wr.wr_id= ( uintptr_t ) task;
	// send_wr.opcode=IBV_WR_RDMA_WRITE;
	// send_wr.sg_list=&sge;
	// send_wr.num_sge=1;
	// send_wr.send_flags=IBV_SEND_SIGNALED;
	// send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->Herd.S_mr)->addr + wwork->cur * buffer_size;
	// if(wwork->flag_write == 0)
 //        send_wr.wr.rdma.remote_addr += wwork->length;
	// send_wr.wr.rdma.rkey= (&client->Herd.S_mr)->rkey;
	// sge.addr= ( uintptr_t ) local_smr->mr->addr;
	// sge.length= head_size;
	// sge.lkey= local_smr->mr->lkey;
	// int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	// if ( err )
	// {
	// 	ERROR_LOG("ibv_post_send error");
	// 	exit(-1);
	// 	return -1;
	// }	

	// while(!task->done_flag);	
	// free(task);
	// if(local_smr != NULL)
	// {
	// 	ibv_dereg_mr(local_smr->mr);
	// 	free(local_smr);
	// }
	// wwork->done_flag=true;
	
}

int amper_FaRM_work_handler(struct dhmp_work *work)   
{
	struct amper_L5_work *wwork;
	wwork=(struct amper_L5_work *)work->work_data;
	struct dhmp_send_mr* local_smr = NULL ;
	
	size_t head_size = sizeof(uintptr_t)*2 + sizeof(size_t)+2;
	size_t buffer_size = head_size + wwork->length;
	void *temp_head ,*temp_all;

	if(wwork->flag_write == 1)
		head_size += wwork->length;
	temp_all = client->FaRM.local_mr[wwork->cur]->addr;
	temp_head = temp_all;
	if(wwork->flag_write == 1)
	{
		memcpy(temp_all, wwork->local_addr , wwork->length);
		temp_head += wwork->length;
	}
	memcpy(temp_head, &(wwork->dhmp_addr) ,sizeof(uintptr_t));
	memcpy(temp_head + sizeof(uintptr_t), &(wwork->local_addr) ,sizeof(uintptr_t));
	memcpy(temp_head + sizeof(uintptr_t)*2, &(wwork->length), sizeof(size_t));
	memcpy(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t), &(wwork->flag_write), 1);
	*(char*)(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t)+1) = 1;//valid
	

	struct dhmp_task* task;
	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->FaRM.S_mr)->addr + wwork->cur * buffer_size;
	if(wwork->flag_write == 0)
        send_wr.wr.rdma.remote_addr += wwork->length;
	send_wr.wr.rdma.rkey= (&client->FaRM.S_mr)->rkey;
	sge.addr= ( uintptr_t ) client->FaRM.local_mr[wwork->cur]->addr;
	sge.length= head_size;
	sge.lkey= client->FaRM.local_mr[wwork->cur]->lkey;
	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	

	while(!task->done_flag);	
	free(task);
	wwork->done_flag=true;
	
}



void *FaRM_run_client() // 本地core dump的原因
{
	int i = 0;
	char * reply, * valid;
	char write_flag;
	void* local_addr;
	size_t buffer_size =  sizeof(uintptr_t)*2 + sizeof(size_t) + 2 + client->FaRM.size;
	reply = client->FaRM.C_mr->addr + client->FaRM.size;
	valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
	while(1)
	{
		if(*valid == 0)
			continue;
		*valid = 0;
		local_addr = (void*)*(uintptr_t*)(reply + sizeof(uintptr_t));
		write_flag = *(reply + sizeof(uintptr_t)*2 + sizeof(size_t));
		if(write_flag == 0)
			memcpy(local_addr , client->FaRM.C_mr->addr + buffer_size * client->FaRM.Ccur , client->FaRM.size);

		pthread_mutex_lock(&client->mutex_request_num);
		client->FaRM.is_available[client->FaRM.Ccur] = 1;
		pthread_mutex_unlock(&client->mutex_request_num);

		client->FaRM.Ccur = (client->FaRM.Ccur + 1) % FaRM_buffer_NUM;
		reply = client->FaRM.C_mr->addr + client->FaRM.size + client->FaRM.Ccur * buffer_size;
		valid = reply + (sizeof(uintptr_t)*2 + sizeof(size_t) + 1);
		if(client->FaRM.size == 0)
		{
			break;
		}
	}
	return;
}



int amper_pRDMA_WS_work_handler(struct dhmp_work *work)   
{
	struct amper_L5_work *wwork;
	wwork=(struct amper_L5_work *)work->work_data;
	struct dhmp_send_mr* local_smr = NULL ;
	
	size_t head_size = sizeof(uintptr_t)*2 + sizeof(size_t)+2;
	size_t buffer_size = head_size + wwork->length;
	void *temp_head ,*temp_all;

	if(wwork->flag_write == 1)
		head_size += wwork->length;
	temp_all = client->pRDMA.C_mr[wwork->cur]->addr;
	temp_head = temp_all;
	if(wwork->flag_write == 1)
	{
		memcpy(temp_all, wwork->local_addr , wwork->length);
		temp_head += wwork->length;
	}
	*(uintptr_t*)temp_head =(uintptr_t) wwork->dhmp_addr;
	*(uintptr_t*)(temp_head + sizeof(uintptr_t)) =(uintptr_t) wwork->local_addr;
	*(size_t*)(temp_head + sizeof(uintptr_t)*2) = wwork->length;
	*(char*)(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t)) = wwork->flag_write;
	*(char*)(temp_head + sizeof(uintptr_t)*2 + sizeof(size_t)+1) = 1;//valid

	struct dhmp_task* task;
	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->pRDMA.S_mr)->addr + wwork->cur * buffer_size;
	if(wwork->flag_write == 0)
        send_wr.wr.rdma.remote_addr += wwork->length;
	send_wr.wr.rdma.rkey= (&client->pRDMA.S_mr)->rkey;
	sge.addr= ( uintptr_t ) client->pRDMA.C_mr[wwork->cur]->addr;
	sge.length= head_size;
	sge.lkey= client->pRDMA.C_mr[wwork->cur]->lkey;
	int err=ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	
	//RDMA WFLUSH
	{
		struct dhmp_task* task2;
		task2 = dhmp_read_task_create(wwork->rdma_trans, NULL, 0);
		if(!task2)
		{
			ERROR_LOG("allocate memory error.");
			return -1;
		}
		struct ibv_sge sge;
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.wr_id= ( uintptr_t ) task2;
		send_wr.opcode=IBV_WR_RDMA_READ;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
		send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->pRDMA.S_mr)->addr + (wwork->cur+1) * buffer_size - 1; //last 1 byte write 
		send_wr.wr.rdma.rkey= (&client->pRDMA.S_mr)->rkey;
		sge.addr= ( uintptr_t ) client->pRDMA.read_mr->addr;
		sge.length= 1;
		sge.lkey= client->pRDMA.read_mr->lkey;

		show(&time_start,0);

		ibv_post_send ( wwork->rdma_trans->qp, &send_wr, &bad_wr );
		while(!task2->done_flag);	

		show(&time_end,1);

		free(task2);
	}
	while(!task->done_flag);	
	free(task);
	wwork->done_flag=true;
	
}

void amper_Tailwind_RPC_work_handler(struct dhmp_work *work) 
{
	struct amper_Tailwind_work *wwork;
	wwork=(struct amper_Tailwind_work *)work->work_data;

	struct dhmp_msg msg ;
	struct dhmp_TailwindRPC_request req_msg;
	void * temp;
	size_t totol_offset = wwork->offset * (wwork->length  + wwork->head_size);
	/*build malloc request msg*/
	req_msg.req_size = wwork->length + wwork->head_size;
	req_msg.dhmp_addr = ( void * ) (client->Tailwind_buffer.mr.addr + totol_offset);
	req_msg.task = wwork;

	msg.msg_type=DHMP_MSG_Tailwind_RPC_requeset ;
	
	msg.data_size = sizeof(struct dhmp_TailwindRPC_request) + wwork->length + wwork->head_size;
	temp = malloc(msg.data_size );
	memcpy(temp, &req_msg, sizeof(struct dhmp_TailwindRPC_request));
	memcpy(temp + sizeof(struct dhmp_TailwindRPC_request), wwork->head_mr->addr, wwork->head_size);
	memcpy(temp + sizeof(struct dhmp_TailwindRPC_request) + wwork->head_size, wwork->local_addr, wwork->length);
	msg.data = temp;

	dhmp_post_send(wwork->rdma_trans, &msg);

	/*wait for the server return result*/
	while(!wwork->recv_flag);
	wwork->done_flag=true;
}

int amper_Tailwind_work_handler(struct dhmp_work *work)  
{
	struct amper_Tailwind_work *wwork;
	wwork=(struct amper_Tailwind_work *)work->work_data;
	
	size_t totol_offset = wwork->offset * (wwork->length  + wwork->head_size);
	struct dhmp_send_mr* smr=NULL;
	smr = dhmp_create_smr_per_ops(wwork->rdma_trans, wwork->local_addr, wwork->length);
	struct dhmp_task* task;
	task=dhmp_write_task_create(wwork->rdma_trans, smr, wwork->length);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}

	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge[2];
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=sge;
	send_wr.num_sge=2;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr = ( uintptr_t ) (client->Tailwind_buffer.mr.addr + totol_offset);
	send_wr.wr.rdma.rkey= client->Tailwind_buffer.mr.rkey;

	sge[0].addr= ( uintptr_t ) wwork->head_mr->addr;
	sge[0].length= wwork->head_size;
	sge[0].lkey= wwork->head_mr->lkey;
	sge[1].addr= ( uintptr_t ) task->sge.addr;
	sge[1].length=task->sge.length;
	sge[1].lkey=task->sge.lkey;

	ibv_post_send ( wwork->rdma_trans->qp, &send_wr, &bad_wr );

#ifdef FLUSH1
	{
		struct dhmp_task* task2;
		task2 = dhmp_read_task_create(wwork->rdma_trans, NULL, 0);
		if(!task2)
		{
			ERROR_LOG("allocate memory error.");
			return -1;
		}
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.wr_id= ( uintptr_t ) task2;
		send_wr.opcode=IBV_WR_RDMA_READ;
		send_wr.sg_list=NULL;
		send_wr.num_sge=0;
		send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr= ( uintptr_t ) (client->Tailwind_buffer.mr.addr + totol_offset);
		send_wr.wr.rdma.rkey= client->Tailwind_buffer.mr.rkey;
		ibv_post_send ( wwork->rdma_trans->qp, &send_wr, &bad_wr );
		while(!task2->done_flag);	
		free(task2);
	}	
#endif
#ifdef FLUSH2
	char * flush2_tag = client->local_mr->addr;
	while(*flush2_tag == 0);
	*flush2_tag = 0;
#endif
	while(!task->done_flag);	
	free(task);
	ibv_dereg_mr(smr->mr);
	free(smr);
out:
	wwork->done_flag=true;
}

int amper_DaRPC_work_handler(struct dhmp_work *work) 
{
	struct amper_DaRPC_work *wwork;
	wwork=(struct amper_DaRPC_work *)work->work_data;

	int i = 0;
	size_t count = sizeof(void*) + sizeof(size_t);//valid+write/red +addr+size+data
	if(wwork->flag_write == 1)
		count += wwork->length;
	size_t totol_count = count * wwork->batch;
	char * daprc_data = malloc(sizeof(struct dhmp_DaRPC_request) + 2 + totol_count);

	struct dhmp_DaRPC_request req_msg;
	req_msg.req_size= wwork->length;
	req_msg.local_addr = wwork->local_addr;
	req_msg.task = wwork;
	memcpy(daprc_data, &req_msg, sizeof(struct dhmp_DaRPC_request));
	char * cur_addr = daprc_data + sizeof(struct dhmp_DaRPC_request);
	cur_addr[0] = wwork->batch;
	cur_addr[1] = wwork->flag_write;
	cur_addr = cur_addr+2;
	for(i = 0;i < wwork->batch;i++)
	{
		*(uintptr_t*)(cur_addr) = (wwork->globle_addr)[i];
		*(size_t*)(cur_addr + sizeof(uintptr_t)) = wwork->length;
		cur_addr = cur_addr + sizeof(uintptr_t) + sizeof(size_t);
		if(wwork->flag_write == 1)
		{
			memcpy(cur_addr, (void*)(wwork->local_addr)[i], wwork->length);
			cur_addr += wwork->length;
		}
	}

	struct dhmp_msg msg ;
	msg.msg_type=DHMP_MSG_DaRPC_requeset ;
	msg.data_size = sizeof(struct dhmp_DaRPC_request) + 2 + totol_count;
	msg.data = daprc_data;
	dhmp_post_send(wwork->rdma_trans, &msg);


	// free(daprc_data);
	/*wait for the server return result*/
	// while(!wwork->recv_flag);   //not wait recv here  ,wait for mutex_num-limit
	wwork->done_flag=true;
}



int amper_FaSST_work_handler(struct dhmp_work *work) 
{
	struct dhmp_UD_work *wwork;
	struct dhmp_msg msg;
	struct dhmp_UD_request req_msg;
	wwork=(struct dhmp_UD_work*)work->work_data;
	char batch = wwork->batch;
	size_t length = wwork->length;
	char flag_write = wwork->flag_write;
	int i;
	/*build malloc request msg*/
	req_msg.req_size = length;
	req_msg.task = wwork;
	req_msg.local_addr = wwork->local_addr;

	size_t local_length = sizeof(struct dhmp_UD_request) + 2 + batch * (sizeof(void*) + sizeof(size_t));
	if(flag_write == 1)
		local_length += (batch * length);//(data+remote_addr+size)* batch
	void * local_data = malloc(local_length);
	void * temp = local_data; 
	memcpy(temp, &req_msg , sizeof(struct dhmp_UD_request));
	temp += sizeof(struct dhmp_UD_request);
	*(char *) temp = batch;
	*(char *)(temp+1) = flag_write;
	temp += 2;
	for(i = 0; i < batch ;i++)
	{
		memcpy(temp , &(wwork->globle_addr[i]), sizeof(uintptr_t));
		memcpy(temp + sizeof(uintptr_t) , &length , sizeof(size_t));
		temp  = temp + sizeof(uintptr_t) + sizeof(size_t);
		if(flag_write == 1)
		{
			memcpy(temp , (void*)((wwork->local_addr)[i]), length);
			temp += length;
		}
	}

	msg.msg_type=DHMP_MSG_UD_REQUEST;
	msg.data_size = local_length;
	msg.data = local_data;
	amper_ud_post_send(client->connect_trans, &msg);

	/*wait for the server return result*/
	wwork->done_flag=true;
}


int amper_RFP_work_handler(struct dhmp_work *work)  
{
	struct dhmp_RFP_work *wwork;
	wwork=(struct dhmp_RFP_work *)work->work_data;


	size_t head_size = sizeof(uintptr_t) + sizeof(size_t)+2;
	void *temp_head = client->RFP.local_smr->mr->addr;
	if(wwork->is_write == true)
	{
		memcpy(temp_head , wwork->local_addr, wwork->length);
		temp_head += wwork->length;
		head_size += wwork->length;
		((char*)temp_head)[0] = 1;
	}
	else
		((char*)temp_head)[0] = 0;
	memcpy(temp_head+1, &(wwork->dhmp_addr) ,sizeof(uintptr_t));
	memcpy(temp_head+1 + sizeof(uintptr_t), &(wwork->length), sizeof(size_t));
	*(char *)(temp_head+1 + sizeof(uintptr_t) + sizeof(size_t)) = 1; //valid

	struct dhmp_task* task;
	task = dhmp_write_task_create(wwork->rdma_trans, NULL, 0);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge ;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) (&client->RFP.write_mr)->addr;
	if(wwork->is_write == false)
		send_wr.wr.rdma.remote_addr += wwork->length;
	send_wr.wr.rdma.rkey= (&client->RFP.write_mr)->rkey;
	sge.addr= ( uintptr_t ) client->RFP.local_smr->mr->addr;
	sge.length= head_size;
	sge.lkey= client->RFP.local_smr->mr->lkey;
	int err = ibv_post_send ( task->rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}	

	while(!task->done_flag);
	free(task);
	

	//now get reply
	size_t reply_size;
	reply_size = sizeof(size_t) + 2;
	if(wwork->is_write == false)
		reply_size += wwork->length;
	task=dhmp_read_task_create(wwork->rdma_trans, client->per_ops_mr2, reply_size);
	if(!task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	while(1)
	{
		amper_post_read(task, &client->RFP.read_mr, task->sge.addr, task->sge.length, task->sge.lkey, false);
		while(!task->done_flag);
		task->done_flag = false;
		char* time = client->per_ops_mr2->mr->addr + reply_size - 1;
		if(*time != client->RFP.time) // init = 1
			continue;
		if(client->RFP.time == 1)
			client->RFP.time = 2;
		else
			client->RFP.time = 1;
		if(wwork->is_write == false)
		{
			memcpy(wwork->local_addr, client->per_ops_mr2->mr->addr + sizeof(size_t) + 1 , wwork->length);
		}
		free(task);
		break;
	}
out:
	wwork->done_flag=true;
}



void dhmp_write2_work_handler(struct dhmp_work *work)
{
	struct dhmp_rw2_work *wwork;
	struct dhmp_Write2_request req_msg;
	void * temp;

	wwork=(struct dhmp_rw2_work*)work->work_data;

	/*build malloc request msg*/
	req_msg.is_new = true;
	req_msg.req_size= wwork->length;
	req_msg.dhmp_addr = wwork->dhmp_addr;
	req_msg.task = wwork;
	
	dhmp_post_write2(wwork->rdma_trans, &req_msg, wwork->S_ringMR, wwork->local_addr);

	/*wait for the server return result*/
	while(!wwork->recv_flag);
	wwork->done_flag=true;
}

void dhmp_send_work_handler(struct dhmp_work *work)
{
	struct dhmp_Send_work *Send_work;
	struct dhmp_msg msg ;
	struct dhmp_Send_request req_msg;
	void * temp;

	Send_work=(struct dhmp_Send_work*)work->work_data;

	/*build malloc request msg*/
	req_msg.req_size=Send_work->length;
	req_msg.dhmp_addr = Send_work->dhmp_addr;
	req_msg.task = Send_work;
	req_msg.is_write = Send_work->is_write;
	req_msg.local_addr = Send_work->local_addr;

	msg.msg_type=DHMP_MSG_SEND_REQUEST;
	

	msg.data_size = sizeof(struct dhmp_Send_request) + Send_work->length;
	temp = malloc(msg.data_size );
	memcpy(temp,&req_msg,sizeof(struct dhmp_Send_request));
	memcpy(temp+sizeof(struct dhmp_Send_request),Send_work->local_addr, Send_work->length);
	msg.data = temp;
		
	dhmp_post_send(Send_work->rdma_trans, &msg);

	

	/*wait for the server return result*/
	while(!Send_work->recv_flag);
	Send_work->done_flag=true;
}

void dhmp_send2_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct dhmp_Send2_work *wwork;
	int index;
	
	wwork=(struct dhmp_Send2_work *)work->work_data;
	
	index=dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("write addr is error.");
		goto out;
	}

	/*check no the same addr write task in qp*/
	while(addr_info->write_flag);
	addr_info->write_flag=true;
	
	++addr_info->write_cnt;
	dhmp_rdma_write2(wwork->rdma_trans, addr_info, &addr_info->nvm_mr,
					wwork->local_addr, wwork->length);

	struct dhmp_msg msg ;
	struct dhmp_Send2_request req_msg;

	/*build malloc request msg*/
	req_msg.req_size=wwork->length;
	req_msg.server_addr = wwork->server_addr;
	req_msg.task = wwork;

	msg.msg_type=DHMP_MSG_SEND2_REQUEST;
	
	msg.data_size = sizeof(struct dhmp_Send2_request) ;
	msg.data = &req_msg;

	dhmp_post_send(wwork->rdma_trans, &msg);

	/*wait for the server return result*/
	while(!wwork->recv_flag);
out:
	wwork->done_flag=true;
}

void dhmp_ReqAddr1_work_handler(struct dhmp_work *dwork)
{
	struct reqAddr_work *work;
	struct dhmp_msg msg ;
	struct dhmp_ReqAddr1_request req_msg;

	work=(struct reqAddr_work*)dwork->work_data;

	/*build malloc request msg*/
	req_msg.req_size= work->length;
	req_msg.dhmp_addr = work->dhmp_addr;
	req_msg.task = work;
	req_msg.cmpflag = work->cmpflag;

	msg.msg_type=DHMP_MSG_REQADDR1_REQUEST;  
	
	msg.data_size = sizeof(struct dhmp_ReqAddr1_request);
	msg.data = &req_msg;

	dhmp_post_send(work->rdma_trans, &msg);

	/*wait for the server return result*/
	while(!work->recv_flag);
	work->done_flag=true;
}

void dhmp_WriteImm_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct dhmp_writeImm_work *wwork;
	int index;
	
	wwork=(struct dhmp_writeImm_work *)work->work_data;
	
	index=dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("write addr is error.");
		goto out;
	}

	/*check no the same addr write task in qp*/
	while(addr_info->write_flag);
	addr_info->write_flag=true;
	
	++addr_info->write_cnt;
	dhmp_post_writeImm(wwork->rdma_trans, addr_info, &addr_info->nvm_mr,
					wwork->local_addr, wwork->length, wwork->task_offset);

out:
	wwork->done_flag=true;
}

void dhmp_WriteImm2_work_handler(struct dhmp_work *work)
{
	struct dhmp_addr_info *addr_info;
	struct dhmp_writeImm2_work *wwork;
	int index;
	
	wwork=(struct dhmp_writeImm2_work *)work->work_data;
	
	index=dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);
	if(!addr_info)
	{
		ERROR_LOG("write addr is error.");
		goto out;
	}

	/*check no the same addr write task in qp*/
	while(addr_info->write_flag);
	addr_info->write_flag=true;
	
	++addr_info->write_cnt;
	dhmp_rdma_write2(wwork->rdma_trans, addr_info, &addr_info->nvm_mr,
					wwork->local_addr, wwork->length);

	struct dhmp_WriteImm2_request req_msg;
	void * temp;

	/*build malloc request msg*/
	req_msg.msg_type=DHMP_MSG_WriteImm2_REQUEST;
	req_msg.req_size= wwork->length;
	req_msg.server_addr = wwork->server_addr;
	req_msg.task = wwork;
	
	dhmp_post_writeImm2(wwork->rdma_trans, &req_msg, wwork->S_ringMR, wwork->local_addr);

	/*wait for the server return result*/
	while(!wwork->recv_flag);
out:	
	wwork->done_flag=true;
}

void dhmp_WriteImm3_work_handler(struct dhmp_work *dwork)
{

	struct dhmp_writeImm3_work *work;
	struct dhmp_WriteImm3_request req_msg;
	void * temp;

	work=(struct dhmp_writeImm3_work*)dwork->work_data;

	/*build malloc request msg*/
	req_msg.msg_type=DHMP_MSG_WriteImm3_REQUEST;
	req_msg.req_size= work->length;
	req_msg.dhmp_addr = work->dhmp_addr;
	req_msg.task = work;
	
 dhmp_post_writeImm3(work->rdma_trans, &req_msg, work->S_ringMR, work->local_addr);


	/*wait for the server return result*/
	while(!work->recv_flag);

	work->done_flag=true;
}

void dhmp_sread_work_handler(struct dhmp_work *work)
{
	struct dhmp_Sread_work *wwork;
	struct dhmp_msg msg ;
	struct dhmp_Sread_request req_msg;
	void * temp;

	wwork=(struct dhmp_Sread_work*)work->work_data;

	struct dhmp_addr_info *addr_info;
	int index = dhmp_hash_in_client(wwork->dhmp_addr);
	addr_info=dhmp_get_addr_info_from_ht(index, wwork->dhmp_addr);

	/*build malloc request msg*/
	req_msg.req_size=wwork->length;
	
	req_msg.task = wwork;
	
	memcpy(client->per_ops_mr_addr, wwork->local_addr , wwork->length);
	struct ibv_mr * temp_mr=client->per_ops_mr->mr;
	memcpy(&(req_msg.client_mr) , temp_mr , sizeof(struct ibv_mr));
	memcpy(&(req_msg.server_mr)  , &(addr_info->nvm_mr), sizeof(struct ibv_mr));

	msg.msg_type=DHMP_MSG_Sread_REQUEST;
	msg.data_size = sizeof(struct dhmp_Sread_request);
	msg.data = &req_msg;

	dhmp_post_send(wwork->rdma_trans, &msg);

	/*wait for the server return result*/
	while(!wwork->recv_flag);
error:
	wwork->done_flag=true;
}

void dhmp_close_work_handler(struct dhmp_work *work)
{
	struct dhmp_close_work *cwork;
	struct dhmp_msg msg;
	int tmp=0;
	
	cwork=(struct dhmp_close_work*)work->work_data;

	msg.msg_type=DHMP_MSG_CLOSE_CONNECTION;
	msg.data_size=sizeof(int);
	msg.data=&tmp;
	
	dhmp_post_send(cwork->rdma_trans, &msg);

	cwork->done_flag=true;
}

void *dhmp_work_handle_thread(void *data)
{
	struct dhmp_work *work;

	while(1)
	{
		work=NULL;
		
		pthread_mutex_lock(&client->mutex_work_list);
		if(!list_empty(&client->work_list))
		{
			work=list_first_entry(&client->work_list, struct dhmp_work, work_entry);
			list_del(&work->work_entry);
		}
		pthread_mutex_unlock(&client->mutex_work_list);

		if(work)
		{
			switch (work->work_type)
			{
				case DHMP_WORK_MALLOC:
					dhmp_malloc_work_handler(work);
					break;
				case DHMP_WORK_FREE:
					dhmp_free_work_handler(work);
					break;
				case DHMP_WORK_READ:
					dhmp_read_work_handler(work);
					break;
				case AMPER_WORK_CLOVER:
					amper_clover_work_handler(work);
					break;
				case AMPER_WORK_L5:
					amper_L5_work_handler(work);
					break;
				case AMPER_WORK_FaRM:
					amper_FaRM_work_handler(work);
					break;
				case AMPER_WORK_scalable:
					amper_scalable_work_handler(work);
					break;
				case AMPER_WORK_Tailwind:
					amper_Tailwind_work_handler(work);
					break;	
				case AMPER_WORK_Tailwind_RPC:
					amper_Tailwind_RPC_work_handler(work);
					break;
				case AMPER_WORK_DaRPC:
					amper_DaRPC_work_handler(work);
					break;
				case AMPER_WORK_UD:
					amper_FaSST_work_handler(work);
					break;	
				case AMPER_WORK_RFP:
					amper_RFP_work_handler(work);
					break;
				case AMPER_WORK_Herd:
					amper_herd_work_handler(work);
					break;
				case AMPER_WORK_pRDMA_WS:
					amper_pRDMA_WS_work_handler(work);
					break;
				case DHMP_WORK_WRITE:
					dhmp_write_work_handler(work);
					break;
				case DHMP_WORK_WRITE2:
					dhmp_write2_work_handler(work);
					break;
				case DHMP_WORK_SEND:
					dhmp_send_work_handler(work);
					break;
				case DHMP_WORK_SEND2:
					dhmp_send2_work_handler(work);
					break;
				case DHMP_WORK_SREAD:
					dhmp_sread_work_handler(work);
					break;
				case DHMP_WORK_REQADDR1:
					dhmp_ReqAddr1_work_handler(work);
					break;
				case DHMP_WORK_WRITEIMM:
					dhmp_WriteImm_work_handler(work);
					break;
				case DHMP_WORK_WRITEIMM2:
					dhmp_WriteImm2_work_handler(work);
					break;
				case DHMP_WORK_WRITEIMM3:
					dhmp_WriteImm3_work_handler(work);
					break;
				case DHMP_WORK_CLOSE:
					dhmp_close_work_handler(work);
					break;
				default:
					ERROR_LOG("work exist error.");
					break;
			}
		}
		
	}

	return NULL;
}


