#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>



//IBV_WC_RDMA_WRITE

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

#define NUM_writeImm3 ((uint32_t)-3)
#define NUM_writeImm2 ((uint32_t)-2)

/*declare static function in here*/
static void dhmp_post_recv(struct dhmp_transport* rdma_trans, void *addr);
static void dhmp_post_all_recv(struct dhmp_transport *rdma_trans);
bool dhmp_destroy_dram_entry(void *nvm_addr);

void amper_create_reply_ah(struct dhmp_transport* rdma_trans, struct ibv_wc *wc)//single
{
	if(server == NULL ||( server != NULL && rdma_trans->ah != NULL)) return;
	struct ibv_qp_attr attr;
	struct ibv_qp_init_attr init_attr;
	rdma_trans->ah = ibv_create_ah_from_wc(rdma_trans->device->pd, wc, (void*)rdma_trans->recv_mr.addr,
					 rdma_trans->cm_id->port_num);
	rdma_trans->qp_num = ntohl(wc->imm_data);

	ibv_query_qp(rdma_trans->cm_id->qp, &attr, IBV_QP_QKEY, &init_attr);
	rdma_trans->qkey = attr.qkey;
	INFO_LOG("amper_create_reply_ah  local = %d qp_num=%d qkey=%d", rdma_trans->cm_id->qp->qp_num,rdma_trans->qp_num, rdma_trans->qkey);
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTED;
}

void * nvm_malloc(size_t size)
{
	void *temp;
#ifdef NVM
	temp = numa_alloc_onnode(size,4);
#else
	temp = malloc(size);
#endif
	memset(temp , 0 , size);
	return temp;//numa_alloc_onnode(size,2);
}

void nvm_free(void *addr, size_t size)
{
#ifdef NVM
	return numa_free(addr,size);
#endif
	return free(addr);
}

/**
 *	return the work completion operation code string.
 */
const char* dhmp_wc_opcode_str(enum ibv_wc_opcode opcode)
{
	switch(opcode)
	{
		case IBV_WC_SEND:
			return "IBV_WC_SEND";
		case IBV_WC_RDMA_WRITE:
			return "IBV_WC_RDMA_WRITE";
		case IBV_WC_RDMA_READ:
			return "IBV_WC_RDMA_READ";
		case IBV_WC_COMP_SWAP:
			return "IBV_WC_COMP_SWAP";
		case IBV_WC_FETCH_ADD:
			return "IBV_WC_FETCH_ADD";
		case IBV_WC_BIND_MW:
			return "IBV_WC_BIND_MW";
		case IBV_WC_RECV:
			return "IBV_WC_RECV";
		case IBV_WC_RECV_RDMA_WITH_IMM:
			return "IBV_WC_RECV_RDMA_WITH_IMM";
		default:
			return "IBV_WC_UNKNOWN";
	};
}

static void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
												struct dhmp_msg* msg)
{
	struct dhmp_Send_response response;
	struct dhmp_msg res_msg;
	void * server_addr = NULL;
	void * temp;

	memcpy ( &response.req_info, msg->data, sizeof(struct dhmp_Send_request));
	size_t length = response.req_info.req_size;
	INFO_LOG ( "client operate size %d",length);

	/*get server addr from dhmp_addr*/
	void * dhmp_addr = response.req_info.dhmp_addr;
	server_addr = dhmp_addr;

	res_msg.msg_type=DHMP_MSG_SEND_RESPONSE;
	
	if(response.req_info.is_write == true) 
	{
		res_msg.data_size=sizeof(struct dhmp_Send_response);
		res_msg.data=&response;
		/*may be need mutex*/
		memcpy(server_addr , (msg->data+sizeof(struct dhmp_Send_request)),length);
		_mm_clflush(server_addr);
	}

	dhmp_post_send(rdma_trans, &res_msg);
	return ;

}

static void dhmp_send_response_handler(struct dhmp_transport* rdma_trans,struct dhmp_msg* msg)
{
	struct dhmp_Send_response response_msg;
	struct dhmp_server_addr_info *addr_info;
	memcpy(&response_msg, msg->data, sizeof(struct dhmp_Send_response));

	if(! response_msg.req_info.is_write)
	{
		memcpy(response_msg.req_info.local_addr ,
			 (msg->data+sizeof(struct dhmp_Send_response)),response_msg.req_info.req_size);
	}

	struct dhmp_Send_work * task = response_msg.req_info.task;
	task->recv_flag = true;
	DEBUG_LOG("response send addr %p ",response_msg.req_info.dhmp_addr);
}

/**
 *	below functions about malloc memory in dhmp_server
 */
static bool dhmp_malloc_area(struct dhmp_msg* msg, 
										struct dhmp_mc_response* response_msg,
										size_t length )
{
	struct dhmp_area *area;

	area=dhmp_area_create(length);
	if(!area)
	{
		ERROR_LOG("allocate one area error.");
		return false;
	}
	
	memcpy(&response_msg->mr, area->mr, sizeof(struct ibv_mr));
	response_msg->server_addr = area->mr->addr;

	INFO_LOG("malloc addr %p lkey %ld",
			response_msg->mr.addr, response_msg->mr.lkey);
	
	// snprintf(response_msg->mr.addr,
	// 		response_msg->req_info.req_size, 
	// 		"welcomebj%p", area);

	return true;
}

static void dhmp_malloc_request_handler(struct dhmp_transport* rdma_trans,
												struct dhmp_msg* msg)
{
	struct dhmp_mc_response response;
	struct dhmp_msg res_msg;
	bool res=true;
	
	memcpy ( &response.req_info, msg->data, sizeof(struct dhmp_mc_request));
	INFO_LOG ( "client req size %d",response.req_info.req_size);

	if(response.req_info.is_special == 1)
	{
		struct ibv_mr * mr = dhmp_malloc_poll_area(SINGLE_POLL_RECV_REGION);
		if(mr == NULL)
			goto req_error;
		memcpy(&(response.mr), mr,sizeof(struct ibv_mr));
		response.server_addr = mr->addr;
		server->rdma_trans = rdma_trans;	
	}
	else if(response.req_info.is_special == 2) //for clover
	{
		// struct ibv_mr * mr = dhmp_malloc_poll_area(response.req_info.req_size);//obj_num, for clover c&s
		// if(mr == NULL)
		// 	goto req_error;
		// memcpy(&(response.mr), mr,sizeof(struct ibv_mr));
		// response.server_addr = mr->addr;
		server->rdma_trans = rdma_trans;	
	}
	else if(response.req_info.is_special == 3)// for L5
	{
		memcpy(&(server->L5_message[rdma_trans->node_id].C_mr), &(response.req_info.mr) , sizeof(struct ibv_mr));
		amper_allocspace_for_server(rdma_trans, 3, response.req_info.req_size); // L5
		memcpy(&(response.mr), server->L5_mailbox.mr, sizeof(struct ibv_mr));
		response.mr.addr += rdma_trans->node_id;
		memcpy(&(response.mr2), server->L5_message[rdma_trans->node_id].mr, sizeof(struct ibv_mr));
		// memcpy(&(response.read_mr), server->L5_mailbox.read_mr, sizeof(struct ibv_mr));
		
	}
	else if(response.req_info.is_special == 4)// for tailwind
	{
		memcpy(&(server->Tailwind_buffer[rdma_trans->node_id].flush2_mr), &(response.req_info.mr) , sizeof(struct ibv_mr));
		amper_allocspace_for_server(rdma_trans, 4, (response.req_info.req_size * Tailwind_log_size)); 
		memcpy(&(response.mr), server->Tailwind_buffer[rdma_trans->node_id].mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 5) // for DaRPC
	{
		amper_allocspace_for_server(rdma_trans, 5, 0); 
		memcpy(&(response.read_mr), server->read_mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 6)// for RFP
	{
		amper_allocspace_for_server(rdma_trans, 6, response.req_info.req_size); 
		memcpy(&(response.mr), server->RFP[rdma_trans->node_id].write_mr, sizeof(struct ibv_mr));
		memcpy(&(response.mr2), server->RFP[rdma_trans->node_id].read_mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 7)// for scaleRPC
	{
		amper_allocspace_for_server(rdma_trans, 7, response.req_info.req_size); 
		memcpy(&(server->scaleRPC[rdma_trans->node_id].Cdata_mr), &(response.req_info.mr) , sizeof(struct ibv_mr));
		memcpy(&(response.mr), server->scaleRPC[rdma_trans->node_id].Sreq_mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 8)// for FaRM
	{
		amper_allocspace_for_server(rdma_trans, 8, response.req_info.req_size); 
		memcpy(&(server->FaRM[rdma_trans->node_id].C_mr), &(response.req_info.mr) , sizeof(struct ibv_mr));
		memcpy(&(response.mr), server->FaRM[rdma_trans->node_id].S_mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 9)// for herd
	{
		amper_allocspace_for_server(rdma_trans, 9, response.req_info.req_size); 
		memcpy(&(response.mr), server->herd[rdma_trans->node_id].S_mr, sizeof(struct ibv_mr));
	}
	else if(response.req_info.is_special == 10)// for octo
	{
		memcpy(&(server->octo[rdma_trans->node_id].C_mr), &(response.req_info.mr) , sizeof(struct ibv_mr));
		amper_allocspace_for_server(rdma_trans, 10, response.req_info.req_size); 
		memcpy(&(response.mr), server->octo[rdma_trans->node_id].S_mr, sizeof(struct ibv_mr));
	}
	else
	{
		if(response.req_info.req_size <= SINGLE_AREA_SIZE)
		{
			res=dhmp_malloc_area(msg, &response, response.req_info.req_size);
		}
		if(!res)
			goto req_error;
	}
	
	res_msg.msg_type=DHMP_MSG_MALLOC_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_mc_response);
	res_msg.data=&response;
#ifdef UD
	amper_ud_post_send(rdma_trans, &res_msg); 
#else
	dhmp_post_send ( rdma_trans, &res_msg );
#endif
	rdma_trans->nvm_used_size+=response.mr.length;
	return ;

req_error:
	/*transmit a message of DHMP_MSG_MALLOC_ERROR*/
	res_msg.msg_type=DHMP_MSG_MALLOC_ERROR;
	res_msg.data_size=sizeof(struct dhmp_mc_response);
	res_msg.data=&response;
#ifdef UD
	amper_ud_post_send(rdma_trans, &res_msg); 
#else
	dhmp_post_send ( rdma_trans, &res_msg );
#endif
	

	return ;
}

static void dhmp_malloc_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct dhmp_mc_response response_msg;
	struct dhmp_addr_info *addr_info;

	memcpy(&response_msg, msg->data, sizeof(struct dhmp_mc_response));
	if(response_msg.req_info.is_special == 1)
	{
		memcpy(&(client->ringbuffer.mr), &response_msg.mr, sizeof(struct ibv_mr)); 
		client->ringbuffer.length = SINGLE_POLL_RECV_REGION;
		client->ringbuffer.cur = 0;
		DEBUG_LOG("responde poll region size = %ld",SINGLE_POLL_RECV_REGION);
	}
	else if(response_msg.req_info.is_special == 2)
	{
		// memcpy(&(client->clover.mr), &response_msg.mr, sizeof(struct ibv_mr)); 
		// client->clover_point.length = response_msg.req_info.req_size;
		DEBUG_LOG("responde clover region size = %ld",response_msg.req_info.req_size);// assume = obj_num
	}
	else if(response_msg.req_info.is_special == 3)
	{
		memcpy(&(client->L5.mailbox_mr), &response_msg.mr, sizeof(struct ibv_mr));
		memcpy(&(client->L5.message_mr), &response_msg.mr2, sizeof(struct ibv_mr)); 
		// memcpy(&(client->L5.read_mr), &response_msg.read_mr, sizeof(struct ibv_mr)); 
		client->L5.num_1 = 1; // for mailbox
	}
	else if(response_msg.req_info.is_special == 4)
	{
		memcpy(&(client->Tailwind_buffer.mr), &response_msg.mr, sizeof(struct ibv_mr)); 
	}
	else if(response_msg.req_info.is_special == 5)
	{
			memcpy(&(client->read_mr_for_SFlush), &response_msg.mr, sizeof(struct ibv_mr)); 
	}
	else if(response_msg.req_info.is_special == 6)
	{
		memcpy(&(client->RFP.write_mr), &response_msg.mr, sizeof(struct ibv_mr));
		memcpy(&(client->RFP.read_mr), &response_msg.mr2, sizeof(struct ibv_mr)); 
		client->RFP.time = 1;
	}
	else if(response_msg.req_info.is_special == 7)
	{
		memcpy(&(client->scaleRPC.Sreq_mr), &response_msg.mr, sizeof(struct ibv_mr));
		client->scaleRPC.context_swith = 0;
	}
	else if(response_msg.req_info.is_special == 8)// for FaRM
	{
		memcpy(&(client->FaRM.S_mr), &response_msg.mr, sizeof(struct ibv_mr));
		client->FaRM.size = response_msg.req_info.req_size;
		client->FaRM.Scur = 0;
		client->FaRM.Ccur = 0;
		int i;
		for(i = 0;i < FaRM_buffer_NUM;i++)
			client->FaRM.is_available[i] = 1;
		pthread_create(&(client->FaRM.poll_thread), NULL, FaRM_run_client, NULL);
	}
	else if(response_msg.req_info.is_special == 9)// for herd
	{
		memcpy(&(client->herd.S_mr), &response_msg.mr, sizeof(struct ibv_mr));
		client->herd.size = response_msg.req_info.req_size;
		client->herd.Scur = 0;
		client->herd.Ccur = 0;
		int i;
		for(i = 0;i < FaRM_buffer_NUM;i++)
			client->herd.is_available[i] = 1;
	}
	else if(response_msg.req_info.is_special == 10)// for octo
	{
		memcpy(&(client->octo.S_mr), &response_msg.mr, sizeof(struct ibv_mr));
	}
	else{
		addr_info=response_msg.req_info.addr_info;
		memcpy(&addr_info->nvm_mr, &response_msg.mr, sizeof(struct ibv_mr));   
		DEBUG_LOG("response mr addr %p lkey %ld",
				addr_info->nvm_mr.addr, addr_info->nvm_mr.lkey);
	}
	struct dhmp_malloc_work * work = response_msg.req_info.task;
	work->recv_flag = true;
}

static void dhmp_malloc_error_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_mc_response response_msg;
	struct dhmp_addr_info *addr_info;
	
	memcpy( &response_msg, msg->data, sizeof(struct dhmp_mc_response));
	addr_info=response_msg.req_info.addr_info;
	addr_info->nvm_mr.length=response_msg.req_info.req_size;
	addr_info->nvm_mr.addr=NULL;
}

/**
 *	below functions about free memory in dhmp_server
 */
static void dhmp_free_one_area(struct ibv_mr* mr, struct dhmp_transport* rdma_trans)
{
	struct dhmp_area* area;
	void *addr;
	size_t size;
	DEBUG_LOG("free one area %p size %d",mr->addr,mr->length);
	
	list_for_each_entry(area, &server->area_list[rdma_trans->node_id], area_entry)
	{
		if(mr->lkey==area->mr->lkey)
			break;
	}
	
	if((&area->area_entry) != (&server->area_list[rdma_trans->node_id]))
	{
		list_del(&area->area_entry);
		addr=area->mr->addr;
		size = area->size;
		ibv_dereg_mr(area->mr);
		nvm_free(addr, size);
		free(area);
	}
}


static void dhmp_free_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr)
{
	struct ibv_mr* mr;
	struct dhmp_free_request *request_msg_ptr;
	struct dhmp_free_response free_res_msg;
	struct dhmp_msg res_msg;
	
	request_msg_ptr=msg_ptr->data;
	mr=&request_msg_ptr->mr;

	rdma_trans->nvm_used_size-=mr->length;

	dhmp_free_one_area(mr, rdma_trans);

	free_res_msg.addr_info=request_msg_ptr->addr_info;
	//free hash in server
	// void * dhmp_addr = request_msg_ptr->dhmp_addr;
	// int index=dhmp_hash_in_client(dhmp_addr);
	// struct dhmp_server_addr_info* addr_info=dhmp_get_addr_info_from_ht_at_Server(index, dhmp_addr);
	// if(!addr_info)
	// {
	// 	ERROR_LOG("free addr error.");
	// 	goto out;
	// }
	// hlist_del(&addr_info->addr_entry);
	//free hash in server
out:
	res_msg.msg_type=DHMP_MSG_FREE_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_free_response);
	res_msg.data=&free_res_msg;
#ifdef UD
	amper_ud_post_send(rdma_trans, &res_msg); 
#else
	dhmp_post_send(rdma_trans, &res_msg);
#endif

	
}

static void dhmp_free_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_free_response *response_msg_ptr;
	struct dhmp_addr_info *addr_info;

	response_msg_ptr=msg->data;
	addr_info=response_msg_ptr->addr_info;
	addr_info->nvm_mr.addr=NULL;
}



/**
 *	dhmp_wc_recv_handler:handle the IBV_WC_RECV event
 */
static void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans,
										struct dhmp_msg* msg)
{	INFO_LOG("recv type = %d",msg->msg_type);
	switch(msg->msg_type)
	{
		case DHMP_MSG_MALLOC_REQUEST:
			dhmp_malloc_request_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_MALLOC_RESPONSE:
			dhmp_malloc_response_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_MALLOC_ERROR:
			dhmp_malloc_error_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_FREE_REQUEST:
			dhmp_free_request_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_FREE_RESPONSE:
			dhmp_free_response_handler(rdma_trans, msg);
			break;
		case DHMP_MSG_CLOSE_CONNECTION:
			rdma_disconnect(rdma_trans->cm_id);
			break;
		case DHMP_MSG_SEND_REQUEST:
		 	dhmp_send_request_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_SEND_RESPONSE:
		 	dhmp_send_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_SEND2_REQUEST:
		 	dhmp_send2_request_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_SEND2_RESPONSE:
		 	dhmp_send2_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_REQADDR1_REQUEST:
		 	dhmp_ReqAddr1_request_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_REQADDR1_RESPONSE:
		 	dhmp_ReqAddr1_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_WriteImm_RESPONSE:
		 	dhmp_WriteImm_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_WriteImm2_RESPONSE:
		 	dhmp_WriteImm2_response_handler(rdma_trans, msg);
		 	break; 
		case DHMP_MSG_WriteImm3_RESPONSE:
		 	dhmp_WriteImm3_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_Write2_RESPONSE:
		 	dhmp_Write2_response_handler(rdma_trans, msg);
		 	break; 
		case DHMP_MSG_Sread_REQUEST:
		 	dhmp_Sread_request_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_Sread_RESPONSE:
		 	dhmp_Sread_response_handler(rdma_trans, msg);
		 	break;
		case DHMP_MSG_Tailwind_RPC_requeset:
			amper_tailwindRPC_request_handler(rdma_trans,msg);
			break;
		case DHMP_MSG_Tailwind_RPC_response:
			amper_tailwindRPC_response_handler(rdma_trans,msg);
			break;
		case DHMP_MSG_DaRPC_requeset:
			amper_DaRPC_request_handler(rdma_trans,msg);
			break;
		case DHMP_MSG_DaRPC_response:
			amper_DaRPC_response_handler(rdma_trans,msg);
			break;		
		case DHMP_MSG_UD_REQUEST:
			amper_UD_request_handler(rdma_trans,msg);
			break;
		case DHMP_MSG_UD_RESPONSE:
			amper_UD_response_handler(rdma_trans,msg);
			break;	
		case DHMP_MSG_herd_RESPONSE:
			amper_herd_response_handler(rdma_trans,msg);
			break;
		case DHMP_MSG_pRDMA_WS_RESPONSE:
			amper_pRDMA_WS_response_handler(rdma_trans,msg);
			break;
			
		// case 96:
		// 	amper_UD_request_handler(rdma_trans,msg);
		default:
			break;
	}
}

/**
 *	the success work completion handler function
 */
static void dhmp_wc_success_handler(struct ibv_wc* wc)
{
	
	struct dhmp_task *task_ptr;
	struct dhmp_transport *rdma_trans;
	struct dhmp_msg msg;
	task_ptr=(struct dhmp_task*)(uintptr_t)wc->wr_id;
	void * message_cxt = task_ptr->sge.addr;
#ifdef UD
	message_cxt += 40;
#endif
	rdma_trans=task_ptr->rdma_trans;
	if(wc->opcode == IBV_WC_RECV)
	{
		/*read the msg content from the task_ptr sge addr*/
		msg.msg_type=*(enum dhmp_msg_type*)message_cxt;
		msg.data_size=*(size_t*)(message_cxt+sizeof(enum dhmp_msg_type));
		msg.data=message_cxt+sizeof(enum dhmp_msg_type)+sizeof(size_t);
	}
	INFO_LOG("opcode:%s ",dhmp_wc_opcode_str(wc->opcode));
	switch(wc->opcode)
	{
		case IBV_WC_RECV_RDMA_WITH_IMM:
			if(wc->imm_data == NUM_writeImm3)
				dhmp_WriteImm3_request_handler(rdma_trans);
			else if(wc->imm_data == NUM_writeImm2)
				dhmp_WriteImm2_request_handler(rdma_trans);
			else if(wc->imm_data == NUM_octo_req)
				dhmp_octo_request_handler(rdma_trans);
			else if(wc->imm_data == NUM_octo_res)
				dhmp_octo_response_handler(rdma_trans);
			else
			{
				dhmp_WriteImm_request_handler(wc->imm_data);
			}
			dhmp_post_recv(rdma_trans, task_ptr->sge.addr);
		case IBV_WC_SEND:
			task_ptr->done_flag=true;
			break;
		case IBV_WC_RECV:
#ifdef UD
			amper_create_reply_ah(rdma_trans,wc);
#endif
			dhmp_wc_recv_handler(rdma_trans, &msg);

			dhmp_post_recv(rdma_trans, task_ptr->sge.addr);
			break;
		case IBV_WC_RDMA_WRITE:
#ifdef Spwrite
			if(task_ptr->addr_info != NULL)
				task_ptr->addr_info->write_flag=false;
#endif
			task_ptr->done_flag=true;	
			break;
		case IBV_WC_RDMA_READ:
			task_ptr->done_flag=true;
			break;
		case IBV_WC_COMP_SWAP:
			task_ptr->done_flag=true;
			break;
		default:
			ERROR_LOG("unknown opcode:%s",
			            dhmp_wc_opcode_str(wc->opcode));
			break;
	}
}

/**
 *	dhmp_wc_error_handler:handle the error work completion.
 */
static void dhmp_wc_error_handler(struct ibv_wc* wc)
{
	if(wc->status==IBV_WC_WR_FLUSH_ERR)
		INFO_LOG("work request flush");
	else
		ERROR_LOG("wc status is [%s] %s",
		            ibv_wc_status_str(wc->status));
}

/**
 *	dhmp_comp_channel_handler:create a completion channel handler
 *  note:set the following function to the cq handle work completion
 */
static void dhmp_comp_channel_handler(int fd, void* data)
{
	struct dhmp_cq* dcq =(struct dhmp_cq*) data;
	struct ibv_cq* cq;
	void* cq_ctx;
	struct ibv_wc wc;
	int err=0;

	err=ibv_get_cq_event(dcq->comp_channel, &cq, &cq_ctx);
	if(err)
	{
		ERROR_LOG("ibv get cq event error.");
		return ;
	}

	ibv_ack_cq_events(dcq->cq, 1);
	err=ibv_req_notify_cq(dcq->cq, 0);
	if(err)
	{
		ERROR_LOG("ibv req notify cq error.");
		return ;
	}

	while(ibv_poll_cq(dcq->cq, 1, &wc))
	{
		if(wc.status==IBV_WC_SUCCESS)
			dhmp_wc_success_handler(&wc);
		else
			dhmp_wc_error_handler(&wc);
	}
}

/*
 *	get the cq because send queue and receive queue need to link it
 */

static struct dhmp_cq* dhmp_cq_get(struct dhmp_device* device, struct dhmp_context* ctx)
{
	struct dhmp_cq* dcq;
	int retval,flags=0;

	dcq=(struct dhmp_cq*) calloc(1,sizeof(struct dhmp_cq));
	if(!dcq)
	{
		ERROR_LOG("allocate the memory of struct dhmp_cq error.");
		return NULL;
	}

	dcq->comp_channel=ibv_create_comp_channel(device->verbs);
	if(!dcq->comp_channel)
	{
		ERROR_LOG("rdma device %p create comp channel error.", device);
		goto cleanhcq;
	}

	flags=fcntl(dcq->comp_channel->fd, F_GETFL, 0);
	if(flags!=-1)
		flags=fcntl(dcq->comp_channel->fd, F_SETFL, flags|O_NONBLOCK);

	if(flags==-1)
	{
		ERROR_LOG("set hcq comp channel fd nonblock error.");
		goto cleanchannel;
	}

	dcq->ctx=ctx;
	retval=dhmp_context_add_event_fd(dcq->ctx,
									EPOLLIN,
									dcq->comp_channel->fd,
									dcq, dhmp_comp_channel_handler);
	if(retval)
	{
		ERROR_LOG("context add comp channel fd error.");
		goto cleanchannel;
	}

	dcq->cq=ibv_create_cq(device->verbs, 100, dcq, dcq->comp_channel, 0);
	if(!dcq->cq)
	{
		ERROR_LOG("ibv create cq error.");
		goto cleaneventfd;
	}

	retval=ibv_req_notify_cq(dcq->cq, 0);
	if(retval)
	{
		ERROR_LOG("ibv req notify cq error.");
		goto cleaneventfd;
	}

	dcq->device=device;
	return dcq;

cleaneventfd:
	dhmp_context_del_event_fd(ctx, dcq->comp_channel->fd);

cleanchannel:
	ibv_destroy_comp_channel(dcq->comp_channel);

cleanhcq:
	free(dcq);

	return NULL;
}

static int amper_ud_qp_create(struct dhmp_transport* rdma_trans)
{
	int retval=0;
	struct ibv_qp_init_attr qp_init_attr;
	struct dhmp_cq* dcq;
	dcq=dhmp_cq_get(rdma_trans->device, rdma_trans->ctx);
	if(!dcq)
	{
		ERROR_LOG("amper cq get error.");
		return -1;
	}
	memset(&qp_init_attr,0,sizeof(qp_init_attr));
	qp_init_attr.qp_context=rdma_trans;
	qp_init_attr.qp_type=IBV_QPT_UD;
	qp_init_attr.send_cq=dcq->cq;
	qp_init_attr.recv_cq=dcq->cq;

	qp_init_attr.cap.max_send_wr=1500;
	qp_init_attr.cap.max_send_sge=2;

	qp_init_attr.cap.max_recv_wr=1500;
	qp_init_attr.cap.max_recv_sge=1;


	retval=rdma_create_qp(rdma_trans->cm_id,
	                        rdma_trans->device->pd,
	                        &qp_init_attr);
	if(retval)
	{
		fprintf(stderr, "Error, rdma_create_qp() failed: %s\n", strerror(errno)); 
		ERROR_LOG("rdma create qp error.");
		goto cleanhcq;
	}
	INFO_LOG("create qp success!");
	rdma_trans->qp=rdma_trans->cm_id->qp;
	rdma_trans->dcq=dcq;

	return retval;

cleanhcq:
	free(dcq);
	return retval;
}

/*
 *	create the qp resource for the RDMA connection
 */
static int dhmp_qp_create(struct dhmp_transport* rdma_trans)
{
	int retval=0;
	struct ibv_qp_init_attr qp_init_attr;
	struct dhmp_cq* dcq;
#ifdef DaRPC_SERVER
	int index = rdma_trans->node_id / DaRPC_clust_NUM;
	
	if( server->DaRPC_dcq[index] == NULL)
	{
		server->DaRPC_dcq[index] = dhmp_cq_get(rdma_trans->device, rdma_trans->ctx); 
		if(!server->DaRPC_dcq[index])
		{
			ERROR_LOG("dhmp cq get error.");
			return -1;
		}
	}
	dcq = server->DaRPC_dcq[index];
	server->DaRPC_dcq_count[index]++;
#else
	dcq=dhmp_cq_get(rdma_trans->device, rdma_trans->ctx);
	if(!dcq)
	{
		ERROR_LOG("dhmp cq get error.");
		return -1;
	}
#endif
	memset(&qp_init_attr,0,sizeof(qp_init_attr));
	qp_init_attr.qp_context=rdma_trans;

	qp_init_attr.qp_type=IBV_QPT_RC;
	qp_init_attr.send_cq=dcq->cq;
	qp_init_attr.recv_cq=dcq->cq;

	qp_init_attr.cap.max_send_wr=15000;
	qp_init_attr.cap.max_send_sge=2;

	qp_init_attr.cap.max_recv_wr=15000;
	qp_init_attr.cap.max_recv_sge=1;
	
	retval=rdma_create_qp(rdma_trans->cm_id,
	                        rdma_trans->device->pd,
	                        &qp_init_attr);
	if(retval)
	{
		ERROR_LOG("rdma create qp error.");
		goto cleanhcq;
	}

	rdma_trans->qp=rdma_trans->cm_id->qp;
	rdma_trans->dcq=dcq;

	return retval;

cleanhcq:
	free(dcq);
	return retval;
}

static void dhmp_qp_release(struct dhmp_transport* rdma_trans)
{
	if(rdma_trans->qp)
	{
		ibv_destroy_qp(rdma_trans->qp);
		ibv_destroy_cq(rdma_trans->dcq->cq);
		dhmp_context_del_event_fd(rdma_trans->ctx,
								rdma_trans->dcq->comp_channel->fd);
		free(rdma_trans->dcq);
		rdma_trans->dcq=NULL;
	}
}


static int on_cm_addr_resolved(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	int retval=0;
	retval=rdma_resolve_route(rdma_trans->cm_id, ROUTE_RESOLVE_TIMEOUT);
	if(retval)
	{
		ERROR_LOG("RDMA resolve route error.");
		return retval;
	}
	return retval;
}


static int on_cm_route_resolved(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	INFO_LOG(" on_cm_route_resolved event->id = %p %d listen=%p %d",event->id,event->id->qp_type,rdma_trans->cm_id, rdma_trans->cm_id->qp_type);
	struct rdma_conn_param conn_param;
	int i, retval=0;
#ifdef UD
	retval=amper_ud_qp_create(rdma_trans);
#else
	retval=dhmp_qp_create(rdma_trans);
#endif
	if(retval)
	{
		ERROR_LOG("hmr qp create error.");
		return retval;
	}

	memset(&conn_param, 0, sizeof(conn_param));
#ifndef UD
	conn_param.retry_count=100;
	conn_param.rnr_retry_count=200;
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
#endif
	retval=rdma_connect(rdma_trans->cm_id, &conn_param);
	if(retval)
	{
		ERROR_LOG("rdma connect error.");
		goto cleanqp;
	}

	dhmp_post_all_recv(rdma_trans);
	return retval;

cleanqp:
	dhmp_qp_release(rdma_trans);
	rdma_trans->ctx->stop=1;
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_ERROR;
	return retval;
}

static struct dhmp_transport* dhmp_is_exist_connection(struct sockaddr_in *sock)
{
	char cur_ip[INET_ADDRSTRLEN], travers_ip[INET_ADDRSTRLEN];
	struct dhmp_transport *rdma_trans=NULL, *res_trans=NULL;
	struct in_addr in=sock->sin_addr;
	int cur_ip_len,travers_ip_len;
	
	inet_ntop(AF_INET, &(sock->sin_addr), cur_ip, sizeof(cur_ip));
	cur_ip_len=strlen(cur_ip);
	
	pthread_mutex_lock(&server->mutex_client_list);
	list_for_each_entry(rdma_trans, &server->client_list, client_entry)
	{
		inet_ntop(AF_INET, &(rdma_trans->peer_addr.sin_addr), travers_ip, sizeof(travers_ip));
		travers_ip_len=strlen(travers_ip);
		
		if(memcmp(cur_ip, travers_ip, max(cur_ip_len,travers_ip_len))==0)
		{
			INFO_LOG("find the same connection.");
			res_trans=rdma_trans;
			break;
		}
	}
	pthread_mutex_unlock(&server->mutex_client_list);

	return res_trans;
}

static int on_cm_connect_request(struct rdma_cm_event* event, 
										struct dhmp_transport* rdma_trans)   //only for ud client  not ud server
{
	INFO_LOG(" on_cm_connect_request event->id = %p %d listen=%p %d",event->id,event->id->qp_type,rdma_trans->cm_id, rdma_trans->cm_id->qp_type);
	struct dhmp_transport* new_trans,*normal_trans;
	struct rdma_conn_param conn_param;
	int i,retval=0;

	normal_trans=dhmp_is_exist_connection(&event->id->route.addr.dst_sin);
	if(normal_trans)
		new_trans=dhmp_transport_create(rdma_trans->ctx, rdma_trans->device,
									false, true);
	else
		new_trans=dhmp_transport_create(rdma_trans->ctx, rdma_trans->device,
									false, false);
	if(!new_trans)
	{
		ERROR_LOG("rdma trans process connect request error.");
		return -1;
	}
	
	new_trans->cm_id=event->id;
	event->id->context=new_trans;

#ifdef UD
	retval=amper_ud_qp_create(new_trans);
#else
	retval = dhmp_qp_create(new_trans);
#endif
	if(retval)
	{
		ERROR_LOG("dhmp qp create error.");
		goto out;
	}

	new_trans->node_id = server->cur_connections;
	++server->cur_connections;
	server->connect_trans[new_trans->node_id] = new_trans;

	pthread_mutex_lock(&server->mutex_client_list);
	list_add_tail(&new_trans->client_entry, &server->client_list);
	pthread_mutex_unlock(&server->mutex_client_list);
	
	memset(&conn_param, 0, sizeof(conn_param));
#ifndef UD
	conn_param.retry_count=100;
	conn_param.rnr_retry_count=200;
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
#endif
	

#ifdef UD
	conn_param.qp_num = new_trans->cm_id->qp->qp_num;
#endif
	retval=rdma_accept(new_trans->cm_id, &conn_param);
	if(retval)
	{
		ERROR_LOG("rdma accept error.");
		return -1;
	}
	
	server->client_num ++;
	new_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTING;
	dhmp_post_all_recv(new_trans);

	return retval;

out:
	free(new_trans);
	return retval;
}

static int on_ud_get_ah(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	struct rdma_conn_param conn_param;
	int i, retval=0;

	rdma_trans->qp_num = event->param.ud.qp_num;
	rdma_trans->qkey = event->param.ud.qkey;

	rdma_trans->ah = ibv_create_ah(rdma_trans->device->pd, &event->param.ud.ah_attr);
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTED;
	INFO_LOG("UD start  local = %d qp_num=%d qkey=%d", rdma_trans->cm_id->qp->qp_num,rdma_trans->qp_num, rdma_trans->qkey);
	return retval;
}

static int on_cm_established(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	int retval=0;

	memcpy(&rdma_trans->local_addr,
			&rdma_trans->cm_id->route.addr.src_sin,
			sizeof(rdma_trans->local_addr));

	memcpy(&rdma_trans->peer_addr,
			&rdma_trans->cm_id->route.addr.dst_sin,
			sizeof(rdma_trans->peer_addr));
#ifdef UD
	on_ud_get_ah(event , rdma_trans);
#endif

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTED;
	return retval;
}

/**
 *	dhmp_destroy_source: destroy the used RDMA resouces
 */
static void dhmp_destroy_source(struct dhmp_transport* rdma_trans)
{
	if(rdma_trans->send_mr.addr)
	{
		ibv_dereg_mr(rdma_trans->send_mr.mr);
		nvm_free(rdma_trans->send_mr.addr,rdma_trans->send_recv_size);
	}

	if(rdma_trans->recv_mr.addr)
	{
		ibv_dereg_mr(rdma_trans->recv_mr.mr);
		nvm_free(rdma_trans->recv_mr.addr ,rdma_trans->send_recv_size);
	}
	
	rdma_destroy_qp(rdma_trans->cm_id);
#ifdef DaRPC_SERVER
	server->DaRPC_dcq_count[rdma_trans->node_id / DaRPC_clust_NUM] -- ;
	if(server->DaRPC_dcq_count[rdma_trans->node_id / DaRPC_clust_NUM] == 0)
#endif
		dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->dcq->comp_channel->fd);

	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->event_channel->fd);
#ifdef RFP
	if(server!= NULL)
		server->RFP[rdma_trans->node_id].time = 0;
#endif
#ifdef scaleRPC
	if(server!= NULL)
		server->scaleRPC[rdma_trans->node_id].Slocal_mr = NULL;
#endif
#ifdef FaRM
	if(client!= NULL)client->FaRM.size = 0;
	if(server!= NULL)server->FaRM[rdma_trans->node_id].size = 0;
#endif
	
}

static int on_cm_disconnected(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	dhmp_destroy_source(rdma_trans);
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_DISCONNECTED;
	if(server!=NULL)
	{
		--server->cur_connections;
		pthread_mutex_lock(&server->mutex_client_list);
		list_del(&rdma_trans->client_entry);
		pthread_mutex_unlock(&server->mutex_client_list);
	}
	
	return 0;
}

static int on_cm_error(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	dhmp_destroy_source(rdma_trans);
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_ERROR;
	if(server!=NULL)
	{
		--server->cur_connections;
		pthread_mutex_lock(&server->mutex_client_list);
		list_del(&rdma_trans->client_entry);
		pthread_mutex_unlock(&server->mutex_client_list);
	}
	return 0;
}

/*
 *	the function use for handling the event of event channel
 */
static int dhmp_handle_ec_event(struct rdma_cm_event* event)
{
	int retval=0;
	struct dhmp_transport* rdma_trans;
	
	rdma_trans=(struct dhmp_transport*) event->id->context;

	INFO_LOG("cm event [%s],status:%d",
	           rdma_event_str(event->event),event->status);

	switch(event->event)
	{
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			retval=on_cm_addr_resolved(event, rdma_trans);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			retval=on_cm_route_resolved(event, rdma_trans);
			break;
		case RDMA_CM_EVENT_CONNECT_REQUEST: 
			retval=on_cm_connect_request(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			retval=on_cm_established(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			retval=on_cm_disconnected(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_CONNECT_ERROR:
			retval=on_cm_error(event, rdma_trans);
			break;
		default:
			//ERROR_LOG("occur the other error.");
			retval=-1;
			break;
	};

	return retval;
}


static void dhmp_event_channel_handler(int fd, void* data)
{
	struct rdma_event_channel* ec=(struct rdma_event_channel*) data;
	struct rdma_cm_event* event,event_copy;
	int retval=0;

	event=NULL;
	while(( retval=rdma_get_cm_event(ec, &event) ==0))
	{
		memcpy(&event_copy, event, sizeof(*event));

		/*
		 * note: rdma_ack_cm_event function will clear event content
		 * so need to copy event content into event_copy.
		 */
		rdma_ack_cm_event(event);

		if(dhmp_handle_ec_event(&event_copy))
			break;
	}

	if(retval && errno!=EAGAIN)
	{
		ERROR_LOG("rdma get cm event error.");
	}
}

static int dhmp_event_channel_create(struct dhmp_transport* rdma_trans)
{
	int flags,retval=0;

	rdma_trans->event_channel=rdma_create_event_channel();
	if(!rdma_trans->event_channel)
	{
		ERROR_LOG("rdma create event channel error.");
		return -1;
	}

	flags=fcntl(rdma_trans->event_channel->fd, F_GETFL, 0);
	if(flags!=-1)
		flags=fcntl(rdma_trans->event_channel->fd,
		              F_SETFL, flags|O_NONBLOCK);

	if(flags==-1)
	{
		retval=-1;
		ERROR_LOG("set event channel nonblock error.");
		goto clean_ec;
	}

	dhmp_context_add_event_fd(rdma_trans->ctx,
								EPOLLIN,
	                            rdma_trans->event_channel->fd,
	                            rdma_trans->event_channel,
	                            dhmp_event_channel_handler);
	return retval;

clean_ec:
	rdma_destroy_event_channel(rdma_trans->event_channel);
	return retval;
}

static int dhmp_memory_register(struct ibv_pd *pd, 
									struct dhmp_mr *dmr, size_t length)
{
	dmr->addr=nvm_malloc(length);
	if(!dmr->addr)
	{
		ERROR_LOG("allocate mr memory error.");
		return -1;
	}

	dmr->mr=ibv_reg_mr(pd, dmr->addr, length, IBV_ACCESS_LOCAL_WRITE);
	if(!dmr->mr)
	{
		ERROR_LOG("rdma register memory error.");
		goto out;
	}

	dmr->cur_pos=0;
	return 0;

out:
	nvm_free(dmr->addr,length);
	return -1;
}

struct dhmp_transport* dhmp_transport_create(struct dhmp_context* ctx, 
													struct dhmp_device* dev,
													bool is_listen,
													bool is_poll_qp)
{
	struct dhmp_transport *rdma_trans;
	int err=0;
	
	rdma_trans=(struct dhmp_transport*)malloc(sizeof(struct dhmp_transport));
	if(!rdma_trans)
	{
		ERROR_LOG("allocate memory error");
		return NULL;
	}

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_INIT;
	rdma_trans->ctx=ctx;
	rdma_trans->device=dev;
	rdma_trans->nvm_used_size=0;
	rdma_trans->ah = NULL;
	
	err=dhmp_event_channel_create(rdma_trans);
	if(err)
	{
		ERROR_LOG("dhmp event channel create error");
		goto out;
	}

	if(!is_listen)
	{
		err=dhmp_memory_register(rdma_trans->device->pd,
								&rdma_trans->send_mr,
								SEND_REGION_SIZE);
		if(err)
			goto out_event_channel;

		err=dhmp_memory_register(rdma_trans->device->pd,
								&rdma_trans->recv_mr,
								RECV_REGION_SIZE);
		if(err)
			goto out_send_mr;
		rdma_trans->send_recv_size = SEND_REGION_SIZE;
		rdma_trans->is_poll_qp=is_poll_qp;
	}
	
	return rdma_trans;
out_send_mr:
	ibv_dereg_mr(rdma_trans->send_mr.mr);
	nvm_free(rdma_trans->send_mr.addr , SEND_REGION_SIZE);
	
out_event_channel:
	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->event_channel->fd);
	rdma_destroy_event_channel(rdma_trans->event_channel);
	
out:
	free(rdma_trans);
	return NULL;
}

int dhmp_transport_listen_UD(struct dhmp_transport* rdma_trans, int listen_port)
{
	int retval=0, backlog;
	struct sockaddr_in addr;
	retval=rdma_create_id(rdma_trans->event_channel,
	                        &rdma_trans->cm_id,
	                        rdma_trans, RDMA_PS_UDP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		return retval;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(listen_port);

	retval=rdma_bind_addr(rdma_trans->cm_id,
	                       (struct sockaddr*) &addr);
	if(retval)
	{
		ERROR_LOG("rdma bind addr error.");
		goto cleanid;
	}

	backlog=10;
	retval=rdma_listen(rdma_trans->cm_id, backlog);
	if(retval)
	{
		ERROR_LOG("rdma listen error.");
		goto cleanid;
	}

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_LISTEN;
	INFO_LOG("rdma listening on port %d",
	           ntohs(rdma_get_src_port(rdma_trans->cm_id)));

	return retval;

cleanid:
	rdma_destroy_id(rdma_trans->cm_id);
	rdma_trans->cm_id=NULL;

	return retval;
}

int dhmp_transport_listen(struct dhmp_transport* rdma_trans, int listen_port)
{
	int retval=0, backlog;
	struct sockaddr_in addr;
	retval=rdma_create_id(rdma_trans->event_channel,
	                        &rdma_trans->cm_id,
	                        rdma_trans, RDMA_PS_TCP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		return retval;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(listen_port);

	retval=rdma_bind_addr(rdma_trans->cm_id,
	                       (struct sockaddr*) &addr);
	if(retval)
	{
		ERROR_LOG("rdma bind addr error.");
		goto cleanid;
	}

	backlog=10;
	retval=rdma_listen(rdma_trans->cm_id, backlog);
	if(retval)
	{
		ERROR_LOG("rdma listen error.");
		goto cleanid;
	}

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_LISTEN;
	INFO_LOG("rdma listening on port %d",
	           ntohs(rdma_get_src_port(rdma_trans->cm_id)));

	return retval;

cleanid:
	rdma_destroy_id(rdma_trans->cm_id);
	rdma_trans->cm_id=NULL;

	return retval;
}

static int dhmp_port_uri_transfer(struct dhmp_transport* rdma_trans,
										const char* url, int port)
{
	struct sockaddr_in peer_addr;
	int retval=0;

	memset(&peer_addr,0,sizeof(peer_addr));
	peer_addr.sin_family=AF_INET;
	peer_addr.sin_port=htons(port);

	retval=inet_pton(AF_INET, url, &peer_addr.sin_addr);
	if(retval<=0)
	{
		ERROR_LOG("IP Transfer Error.");
		goto out;
	}

	memcpy(&rdma_trans->peer_addr, &peer_addr, sizeof(struct sockaddr_in));

out:
	return retval;
}

int dhmp_transport_connect_UD(struct dhmp_transport* rdma_trans,
                             const char* url, int port)
{
	int retval=0;
	if(!url||port<=0)
	{
		ERROR_LOG("url or port input error.");
		return -1;
	}

	retval=dhmp_port_uri_transfer(rdma_trans, url, port);
	if(retval<0)
	{
		ERROR_LOG("rdma init port uri error.");
		return retval;
	}

	/*rdma_cm_id dont init the rdma_cm_id's verbs*/
	retval=rdma_create_id(rdma_trans->event_channel,
						&rdma_trans->cm_id,
						rdma_trans, RDMA_PS_UDP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		goto clean_rdmatrans;
	}
	retval=rdma_resolve_addr(rdma_trans->cm_id, NULL,
	                          (struct sockaddr*) &rdma_trans->peer_addr,
	                           ADDR_RESOLVE_TIMEOUT);
	if(retval)
	{
		ERROR_LOG("RDMA Device resolve addr error.");
		goto clean_cmid;
	}
	
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTING;
	return retval;

clean_cmid:
	rdma_destroy_id(rdma_trans->cm_id);

clean_rdmatrans:
	rdma_trans->cm_id=NULL;

	return retval;
}

int dhmp_transport_connect(struct dhmp_transport* rdma_trans,
                             const char* url, int port)
{
	int retval=0;
	if(!url||port<=0)
	{
		ERROR_LOG("url or port input error.");
		return -1;
	}

	retval=dhmp_port_uri_transfer(rdma_trans, url, port);
	if(retval<0)
	{
		ERROR_LOG("rdma init port uri error.");
		return retval;
	}

	/*rdma_cm_id dont init the rdma_cm_id's verbs*/
	retval=rdma_create_id(rdma_trans->event_channel,
						&rdma_trans->cm_id,
						rdma_trans, RDMA_PS_TCP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		goto clean_rdmatrans;
	}
	retval=rdma_resolve_addr(rdma_trans->cm_id, NULL,
	                          (struct sockaddr*) &rdma_trans->peer_addr,
	                           ADDR_RESOLVE_TIMEOUT);
	if(retval)
	{
		ERROR_LOG("RDMA Device resolve addr error.");
		goto clean_cmid;
	}
	
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTING;
	return retval;

clean_cmid:
	rdma_destroy_id(rdma_trans->cm_id);

clean_rdmatrans:
	rdma_trans->cm_id=NULL;

	return retval;
}

/*
 *	two sided RDMA operations
 */
static void dhmp_post_recv(struct dhmp_transport* rdma_trans, void *addr)
{
	struct ibv_recv_wr recv_wr, *bad_wr_ptr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *recv_task_ptr;
	int err=0;

	if(rdma_trans->trans_state>DHMP_TRANSPORT_STATE_CONNECTED)
		return ;
	
	recv_task_ptr=dhmp_recv_task_create(rdma_trans, addr);
	if(!recv_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	
	recv_wr.wr_id=(uintptr_t)recv_task_ptr;
	recv_wr.next=NULL;
	recv_wr.sg_list=&sge;
	recv_wr.num_sge=1;

	sge.addr=(uintptr_t)recv_task_ptr->sge.addr;
	sge.length=recv_task_ptr->sge.length + sizeof(struct ibv_grh);
	sge.lkey=recv_task_ptr->sge.lkey;
	
	err=ibv_post_recv(rdma_trans->qp, &recv_wr, &bad_wr_ptr);
	if(err)
	{
		fprintf(stderr, "Error, rdma_create_qp() failed: %d %s\n",err, strerror(errno)); 
		ERROR_LOG("ibv post recv error.");
	}
	
}

/**
 *	dhmp_post_all_recv:loop call the dhmp_post_recv function
 */
static void dhmp_post_all_recv(struct dhmp_transport *rdma_trans)
{
	int i, single_region_size=0;

	if(rdma_trans->is_poll_qp)
		single_region_size=SINGLE_POLL_RECV_REGION;
	else
		single_region_size=SINGLE_NORM_RECV_REGION;
	
	for(i=0; i<RECV_REGION_SIZE/single_region_size; i++)
	{
		dhmp_post_recv(rdma_trans, 
			rdma_trans->recv_mr.addr+i*single_region_size);
	}
}

void amper_ud_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr)
{
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *send_task_ptr;
	int err=0;
	
	if(rdma_trans->trans_state!=DHMP_TRANSPORT_STATE_CONNECTED)
		return ;
	send_task_ptr=dhmp_send_task_create(rdma_trans, msg_ptr);
	if(!send_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	
	memset ( &send_wr, 0, sizeof ( send_wr ) );
	send_wr.wr_id= ( uintptr_t ) send_task_ptr;
	send_wr.wr.ud.ah = rdma_trans->ah;
	send_wr.wr.ud.remote_qpn = rdma_trans->qp_num;
	send_wr.wr.ud.remote_qkey = rdma_trans->qkey;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.opcode=IBV_WR_SEND_WITH_IMM;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.imm_data= htonl(rdma_trans->cm_id->qp->qp_num);
	INFO_LOG("imm_data = %d qp_num=%d qkey=%d",rdma_trans->cm_id->qp->qp_num , rdma_trans->qp_num , rdma_trans->qkey);

	sge.addr= ( uintptr_t ) send_task_ptr->sge.addr;
	sge.length=send_task_ptr->sge.length;
	sge.lkey=send_task_ptr->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
		ERROR_LOG ( "ibv_post_send error." );

}

struct dhmp_task * dhmp_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr)
{
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *send_task_ptr;
	int err=0;
	
	if(rdma_trans->trans_state!=DHMP_TRANSPORT_STATE_CONNECTED)
		return ;
	send_task_ptr=dhmp_send_task_create(rdma_trans, msg_ptr);
	if(!send_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	
	memset ( &send_wr, 0, sizeof ( send_wr ) );
	send_wr.wr_id= ( uintptr_t ) send_task_ptr;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.opcode=IBV_WR_SEND;
	send_wr.send_flags=IBV_SEND_SIGNALED;

	sge.addr= ( uintptr_t ) send_task_ptr->sge.addr;
	sge.length=send_task_ptr->sge.length;
	sge.lkey=send_task_ptr->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
		ERROR_LOG ( "ibv_post_send error." );
	
	client->count_post_send_for_SFlush =  (client->count_post_send_for_SFlush + 1 ) %(RECV_REGION_SIZE/SINGLE_NORM_RECV_REGION);
	return send_task_ptr;

}

struct dhmp_send_mr* dhmp_create_smr_per_ops(struct dhmp_transport* rdma_trans, void* addr, int length )
{
	struct dhmp_send_mr *res;

	res=(struct dhmp_send_mr* )malloc(sizeof(struct dhmp_send_mr));
	if(!res)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}
	
	res->mr=ibv_reg_mr(rdma_trans->device->pd,
						addr, length, IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	if(!res->mr)
	{
		ERROR_LOG("ibv register memory error.");
		goto error;
	}
	
	return res;
error:
	free ( res );
	return NULL;
}

int dhmp_rdma_read(struct dhmp_transport* rdma_trans, struct ibv_mr* mr, void* local_addr, int length)
{
	INFO_LOG("dhmp_rdma_read");
	struct dhmp_task* read_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* smr=NULL;
	int err=0;
	smr = dhmp_create_smr_per_ops(rdma_trans, local_addr, length);
	read_task=dhmp_read_task_create(rdma_trans, smr, length);
	if ( !read_task )
	{
		ERROR_LOG ( "allocate memory error." );
		return -1;
	}

	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	send_wr.wr_id= ( uintptr_t ) read_task;
	send_wr.opcode=IBV_WR_RDMA_READ;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr=(uintptr_t)mr->addr;
	send_wr.wr.rdma.rkey=mr->rkey;

	sge.addr=(uintptr_t)read_task->sge.addr;
	sge.length=read_task->sge.length;
	sge.lkey=read_task->sge.lkey;
	
	err=ibv_post_send(rdma_trans->qp, &send_wr, &bad_wr);
	if(err)
	{
		ERROR_LOG("ibv_post_send error");
		goto error;
	}

	DEBUG_LOG("before local addr is %s", local_addr);
	
	while(!read_task->done_flag);
	ibv_dereg_mr(smr->mr);
	free(smr);
	DEBUG_LOG("local addr content is %s", local_addr);

	return 0;
error:
	return -1;
}

int dhmp_rdma_write ( struct dhmp_transport* rdma_trans, struct dhmp_addr_info *addr_info, struct ibv_mr* mr, void* local_addr, int length )
{
	struct dhmp_task* write_task;
	struct dhmp_send_mr* smr=NULL;
	int err=0;
	smr = dhmp_create_smr_per_ops(rdma_trans, local_addr, length);
	write_task=dhmp_write_task_create(rdma_trans, smr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	write_task->addr_info=addr_info;
	amper_post_write(write_task, mr, write_task->sge.addr, write_task->sge.length, write_task->sge.lkey, false);

#ifdef FLUSH1
	struct dhmp_task* read_task;
	read_task=dhmp_read_task_create(rdma_trans, NULL, 0);
	if ( !read_task )
	{
		ERROR_LOG ( "allocate memory error." );
		return -1;
	}
	amper_post_read(read_task, mr, NULL, 0 ,0, false);
	while(!read_task->done_flag);
	free(read_task);
#endif

	while(!write_task->done_flag);
			
	ibv_dereg_mr(smr->mr);
	free(smr);	
	free(write_task);
	
	return 0;
	
error:
	return -1;
}

int getFreeList()
{
	return 0;
}

void dhmp_ReqAddr1_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_ReqAddr1_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, msg->data, sizeof(struct dhmp_ReqAddr1_request));

	res_msg.msg_type=DHMP_MSG_REQADDR1_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_ReqAddr1_response);
	res_msg.data=&response;
#ifdef UD
	amper_ud_post_send(rdma_trans,&res_msg);
#else
	dhmp_post_send(rdma_trans, &res_msg);
#endif

	return ;
}

void dhmp_ReqAddr1_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_ReqAddr1_response * response_msg;
	/*memcpy(&response_msg, msg->data, sizeof(struct dhmp_ReqAddr1_response));*/
	response_msg = msg->data;
	struct reqAddr_work * work = response_msg->req_info.task;

	work->recv_flag = true;
	/*DEBUG_LOG("response ReqAddr1 addr %p ",response_msg.req_info.dhmp_addr); */
}


int dhmp_post_writeImm ( struct dhmp_transport* rdma_trans, struct dhmp_addr_info *addr_info, 
					struct ibv_mr* mr, void* local_addr, int length, uint32_t task_offset)
{
	struct dhmp_task* write_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	int err=0;
	
	memcpy(client->per_ops_mr_addr,local_addr ,length);
	temp_mr=client->per_ops_mr;
	write_task=dhmp_write_task_create(rdma_trans, temp_mr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	write_task->addr_info=addr_info;
	
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) mr->addr;
	send_wr.wr.rdma.rkey=mr->rkey;
	send_wr.imm_data = task_offset;/*(htonl)*/

	sge.addr= ( uintptr_t ) write_task->sge.addr;
	sge.length=write_task->sge.length;
	sge.lkey=write_task->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}
	
	while(!write_task->done_flag);
	
	return 0;
}

void dhmp_WriteImm_request_handler(uint32_t task_offset)
{
	int index = (int) task_offset;
	struct dhmp_WriteImm_response response;
	struct dhmp_msg res_msg;

	response.cmpflag = server->tasklist[index].cmpflag;

	size_t length = server->tasklist[index].length;
	INFO_LOG ( "client writeImm size %d",length);

	/* get server addr from dhmp_addr & copy & flush*/
	void * server_addr =server->tasklist[index].dhmp_addr;
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_WriteImm_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_WriteImm_response);
	res_msg.data=&response;
	
	dhmp_post_send(server->tasklist[index].rdma_trans, &res_msg); /*next use writeImm to responde*/
	
	return ;

}

void dhmp_WriteImm_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_WriteImm_response * response_msg = msg->data;
	/*memcpy(&response_msg, msg->data, sizeof(struct dhmp_WriteImm_response)); */
	*(response_msg->cmpflag) = true;
}


int dhmp_post_writeImm2( struct dhmp_transport* rdma_trans, struct dhmp_WriteImm2_request *msg, 
							struct ibv_mr* mr, void* local_addr)
{
	struct dhmp_task* write_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	int err=0;
	int length ;

	length =  sizeof(struct dhmp_WriteImm2_request);
	memcpy(client->per_ops_mr_addr,msg ,length);
	temp_mr=client->per_ops_mr;
	write_task=dhmp_write_task_create(rdma_trans, temp_mr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) mr->addr;
	send_wr.wr.rdma.rkey=mr->rkey;
	send_wr.imm_data = NUM_writeImm2;/*(htonl)*/

	sge.addr= ( uintptr_t ) write_task->sge.addr;
	sge.length=write_task->sge.length;
	sge.lkey=write_task->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}
	
	while(!write_task->done_flag);
	return 0;

}

void dhmp_WriteImm2_request_handler(struct dhmp_transport* rdma_trans)
{
	struct dhmp_WriteImm2_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, (server->ringbufferAddr + server->cur_addr), sizeof(struct dhmp_WriteImm2_request));
	INFO_LOG ( "client writeImm2 size %d",response.req_info.req_size);

	size_t length = response.req_info.req_size;

	void * server_addr = response.req_info.server_addr;
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_WriteImm2_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_WriteImm2_response);
	res_msg.data=&response;
	
	dhmp_post_send(rdma_trans, &res_msg); /*next use writeImm to responde*/
	
	return ;

}

void dhmp_WriteImm2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_WriteImm2_response * response_msg = msg->data;
	/*memcpy(&response_msg, msg->data, sizeof(struct dhmp_WriteImm2_response)); */
	struct dhmp_writeImm2_work * work = response_msg->req_info.task;

	work->recv_flag = true;
}


int dhmp_post_writeImm3( struct dhmp_transport* rdma_trans, struct dhmp_WriteImm3_request *msg, 
							struct ibv_mr* mr, void* local_addr)
{
	struct dhmp_task* write_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	int err=0;
	
	int length = msg->req_size + sizeof(struct dhmp_WriteImm3_request);
	void * temp = client->per_ops_mr_addr;

	memcpy(temp, msg, sizeof(struct dhmp_WriteImm3_request));
	memcpy(temp + sizeof(struct dhmp_WriteImm3_request), local_addr, msg->req_size);
	temp_mr=client->per_ops_mr;
	write_task=dhmp_write_task_create(rdma_trans, temp_mr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) mr->addr;
	send_wr.wr.rdma.rkey=mr->rkey;
	send_wr.imm_data = NUM_writeImm3;/*(htonl)*/

	sge.addr= ( uintptr_t ) write_task->sge.addr;
	sge.length=write_task->sge.length;
	sge.lkey=write_task->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}
	
	while(!write_task->done_flag);
	
	return 0;

}


void dhmp_WriteImm3_request_handler(struct dhmp_transport* rdma_trans)
{
	INFO_LOG("writeimm3 request handler start");
	struct dhmp_WriteImm3_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, (server->ringbufferAddr + server->cur_addr), sizeof(struct dhmp_WriteImm3_request));
	INFO_LOG ( "client writeImm size %d",response.req_info.req_size);


	void * context_ptr = (void *)(server->ringbufferAddr + server->cur_addr + sizeof(struct dhmp_WriteImm3_request));

	size_t length = response.req_info.req_size;
	void * dhmp_addr = response.req_info.dhmp_addr;
	/* get server addr from dhmp_addr & copy & flush*/
	void * server_addr = dhmp_addr;
	memcpy(server_addr , context_ptr ,length);
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_WriteImm3_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_WriteImm3_response);
	res_msg.data=&response;
	
	dhmp_post_send(rdma_trans, &res_msg); /*next use writeImm to responde*/
	INFO_LOG("writeimm3 request handler over");
	return ;

}

void dhmp_WriteImm3_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_WriteImm3_response * response_msg = msg->data;
	/*memcpy(&response_msg, msg->data, sizeof(struct dhmp_WriteImm3_response)); */
	struct dhmp_writeImm3_work * work = response_msg->req_info.task;

	work->recv_flag = true;
}

int dhmp_rdma_write2 ( struct dhmp_transport* rdma_trans, struct dhmp_addr_info *addr_info, struct ibv_mr* mr, void* local_addr, int length, int dram_flag )
{
	struct dhmp_task* write_task;
	struct dhmp_send_mr* temp_mr=NULL;
	int err=0;
	
	memcpy(client->per_ops_mr_addr,local_addr ,length);
	temp_mr=client->per_ops_mr;
	write_task=dhmp_write_task_create(rdma_trans, temp_mr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	write_task->addr_info=addr_info;
	
	amper_post_write(write_task, mr, write_task->sge.addr, write_task->sge.length, write_task->sge.lkey, false);

	while(!write_task->done_flag);
	
	return 0;
}

void dhmp_send2_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_Send2_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, msg->data, sizeof(struct dhmp_Send2_request));
	size_t length = response.req_info.req_size;
	INFO_LOG ( "client operate size %d",length);

	void * server_addr = response.req_info.server_addr;
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_SEND2_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_Send2_response);
	res_msg.data=&response;

	dhmp_post_send(rdma_trans, &res_msg);
	return ;
}

void dhmp_send2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_Send2_response* response_msg = msg->data;
	struct dhmp_Send2_work * task = response_msg->req_info.task;
	task->recv_flag = true;
	DEBUG_LOG("response flush %p",response_msg->req_info.server_addr);
}

int dhmp_post_write2( struct dhmp_transport* rdma_trans, struct dhmp_Write2_request *msg, 
							struct ibv_mr* mr, void* local_addr)
{
	struct dhmp_task* write_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* temp_mr=NULL;
	int err=0;
	
	int length = msg->req_size + sizeof(struct dhmp_Write2_request);
	void * temp = client->per_ops_mr_addr;
	memcpy(temp, msg, sizeof(struct dhmp_Write2_request));
	memcpy(temp + sizeof(struct dhmp_Write2_request), local_addr, msg->req_size);
	temp_mr=client->per_ops_mr;
	write_task=dhmp_write_task_create(rdma_trans, temp_mr, length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.opcode=IBV_WR_RDMA_WRITE;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) mr->addr;
	send_wr.wr.rdma.rkey=mr->rkey;

	sge.addr= ( uintptr_t ) write_task->sge.addr;
	sge.length=write_task->sge.length;
	sge.lkey=write_task->sge.lkey;

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		return -1;
	}
	
	while(!write_task->done_flag);
	return 0;

}


void dhmp_Write2_request_handler()
{

	struct dhmp_Write2_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, (server->ringbufferAddr + server->cur_addr), sizeof(struct dhmp_Write2_request));
	INFO_LOG ( "client write2 size %d",response.req_info.req_size);

	void * context_ptr = (void *)(server->ringbufferAddr + server->cur_addr +sizeof(struct dhmp_Write2_request));
	size_t length = response.req_info.req_size;
	void * server_addr = response.req_info.dhmp_addr;
	memcpy(server_addr, context_ptr, length);
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_Write2_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_Write2_response);
	res_msg.data=&response;
	dhmp_post_send(server->rdma_trans, &res_msg); /*next use writeImm to responde*/
	return ;

}

void dhmp_Write2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_Write2_response * response_msg = msg->data;
	/*memcpy(&response_msg, msg->data, sizeof(struct dhmp_Write2_response)); */
	struct dhmp_rw2_work * work = response_msg->req_info.task;

	work->recv_flag = true;
}

void dhmp_Sread_read(struct dhmp_transport* rdma_trans, struct ibv_mr* rmr, struct ibv_mr* lmr, int length)
{
	struct dhmp_task* read_task = malloc(sizeof(struct dhmp_task));
	struct ibv_send_wr send_wr2,*bad_wr2=NULL;
	struct ibv_sge sge2;

	memset(&send_wr2, 0, sizeof(struct ibv_send_wr));

	read_task->rdma_trans = rdma_trans;
	read_task->done_flag = false;

	send_wr2.wr_id= ( uintptr_t ) read_task;
	send_wr2.opcode=IBV_WR_RDMA_READ;
	send_wr2.sg_list=&sge2;
	send_wr2.num_sge=1; // or 0
	send_wr2.send_flags=IBV_SEND_SIGNALED;
	send_wr2.wr.rdma.remote_addr=(uintptr_t)rmr->addr;
	send_wr2.wr.rdma.rkey= rmr->rkey;

	sge2.addr=(uintptr_t)lmr->addr;
	sge2.length= length; 
	sge2.lkey= lmr->lkey;
	
	int err=ibv_post_send(rdma_trans->qp, &send_wr2, &bad_wr2);
	if(err)
	{
		ERROR_LOG("ibv_post_send error");
		return ;
	}
	while(!read_task->done_flag);
	free(read_task);			
}

void dhmp_Sread_server()
{
	struct dhmp_Sread_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, (server->ringbufferAddr + server->cur_addr), sizeof(struct dhmp_Sread_request));
	INFO_LOG ( "client sread size %d",response.req_info.req_size);

	/*read*/
	dhmp_Sread_read(server->rdma_trans, &(response.req_info.client_mr), 
				&(response.req_info.server_mr), response.req_info.req_size);

	void * server_addr = response.req_info.server_mr.addr;
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_Sread_RESPONSE;
	res_msg.data_size=sizeof(struct dhmp_Sread_response);
	res_msg.data=&response;

	dhmp_post_send(server->rdma_trans, &res_msg);
	return ;
}

void dhmp_Sread_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_Sread_response response;
	struct dhmp_msg res_msg;

	memcpy ( server->ringbufferAddr, msg->data, sizeof(struct dhmp_Sread_request));
	server->rdma_trans = rdma_trans;
	server->model_C_new_msg = true;
	return ;
}

void dhmp_Sread_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_Sread_response* response_msg = msg->data;
	struct dhmp_Sread_work * task = response_msg->req_info.task;
	task->recv_flag = true;
	DEBUG_LOG("response flush %p",response_msg->req_info.server_mr.addr);
}

void amper_tailwindRPC_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_TailwindRPC_response response;
	struct dhmp_msg res_msg;

	memcpy ( &response.req_info, msg->data, sizeof(struct dhmp_TailwindRPC_request));

	void * context_ptr = msg->data + sizeof(struct dhmp_TailwindRPC_request);
	size_t length = *(size_t *)context_ptr;
	void * server_addr = response.req_info.dhmp_addr;
	memcpy(server_addr, context_ptr + sizeof(size_t) + sizeof(uint32_t) + sizeof(uint32_t), length);
	_mm_clflush(server_addr);

	res_msg.msg_type=DHMP_MSG_Tailwind_RPC_response;
	res_msg.data_size=sizeof(struct dhmp_TailwindRPC_response);
	res_msg.data=&response;
	dhmp_post_send(rdma_trans, &res_msg); 
	return ;
}

void amper_tailwindRPC_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_TailwindRPC_response* response_msg = msg->data;
	struct amper_Tailwind_work * task = response_msg->req_info.task;
	task->recv_flag = true;
	DEBUG_LOG("tailwindRPC flush ");
}

void amper_DaRPC_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	void *response ,*cur_addr;
	size_t reply_size = sizeof(struct dhmp_DaRPC_response);
	
	char * context_ptr = msg->data + sizeof(struct dhmp_DaRPC_request);
	char batch = context_ptr[0];
	char write_flag = context_ptr[1];
	context_ptr += 2;
	size_t size =*(size_t*)(context_ptr + sizeof(uintptr_t));
	void * server_addr;
	int i=0;
	if(write_flag == 0)
		reply_size = reply_size + (batch*size);
	response = malloc(reply_size);
	memcpy ( &((struct dhmp_DaRPC_response *)response)->req_info, msg->data, sizeof(struct dhmp_DaRPC_request));
#if((!defined SFLUSH) && (!defined RFLUSH))	
	((struct dhmp_DaRPC_response *)response)->batch = batch;
	((struct dhmp_DaRPC_response *)response)->write_flag = write_flag;
	cur_addr = response + sizeof(struct dhmp_DaRPC_response);
	for(;i < batch;i++)
	{
		server_addr= (void *)*(uintptr_t*)context_ptr;
		size = *(size_t*)(context_ptr + sizeof(uintptr_t));
		context_ptr = context_ptr + sizeof(uintptr_t) + sizeof(size_t);
		if(write_flag == 1)
		{
			memcpy(server_addr, context_ptr, size);
			_mm_clflush(server_addr);
			context_ptr += size;
		}
		else
		{
			memcpy(cur_addr, server_addr ,size);
			cur_addr += size;
		}
	}
#endif
#if((defined SFLUSH) || (defined RFLUSH))	
	reply_size = sizeof(struct dhmp_DaRPC_response);
#endif
	struct dhmp_msg res_msg;
	res_msg.msg_type=DHMP_MSG_DaRPC_response;
	res_msg.data_size= reply_size;
	res_msg.data= response;
	struct dhmp_task * task = dhmp_post_send(rdma_trans, &res_msg); 
	while(!task->done_flag);
	free(response);
	return ;
}

void amper_DaRPC_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg) // read
{
	struct dhmp_DaRPC_response* response_msg = msg->data;
	struct amper_DaRPC_work * task = response_msg->req_info.task;
#if((!defined SFLUSH) && (!defined RFLUSH))	
	if(response_msg->write_flag == 0)
	{
		void* temp;
		int i;
		temp = msg->data + sizeof(struct dhmp_DaRPC_response);
		for(i = 0;i<response_msg->batch;i++)
		{
			
			memcpy((void*)(response_msg->req_info.local_addr)[0], temp , response_msg->req_info.req_size);
			temp += response_msg->req_info.req_size;
		}
	}
#endif
#ifndef SFLUSH
	task->recv_flag = true;
#endif
#ifdef SFLUSH	
	pthread_mutex_lock(&client->mutex_request_num);
	client->para_request_num ++;
	pthread_mutex_unlock(&client->mutex_request_num);
#endif
}

void amper_UD_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	void *response ,*cur_addr;
	size_t reply_size = sizeof(struct dhmp_UD_response);
	
	char * context_ptr = msg->data + sizeof(struct dhmp_UD_request);
	char batch = context_ptr[0];
	char write_flag = context_ptr[1];
	context_ptr += 2;
	size_t size = *(size_t*)(context_ptr + sizeof(uintptr_t));
	void * server_addr;
	int i=0;
	if(write_flag == 0)
		reply_size = reply_size + (batch*size);
	response = malloc(reply_size);
	memcpy ( &((struct dhmp_UD_response *)response)->req_info, msg->data, sizeof(struct dhmp_UD_request));
	((struct dhmp_UD_response *)response)->batch = batch;
	((struct dhmp_UD_response *)response)->write_flag = write_flag;
	cur_addr = response + sizeof(struct dhmp_UD_response);
	for(;i < batch;i++)
	{
		server_addr= (void *)*(uintptr_t*)context_ptr;
		size = *(size_t*)(context_ptr + sizeof(uintptr_t));
		context_ptr = context_ptr + sizeof(uintptr_t) + sizeof(size_t);
		if(write_flag == 1)
		{
			memcpy(server_addr, context_ptr, size);
			_mm_clflush(server_addr);
			context_ptr += size;
		}
		else
		{
			memcpy(cur_addr, server_addr ,size);
			cur_addr += size;
		}
	}
	struct dhmp_msg res_msg;
	res_msg.msg_type=DHMP_MSG_UD_RESPONSE;
	res_msg.data_size= reply_size;
	res_msg.data= response;
	amper_ud_post_send(rdma_trans, &res_msg); 
	return ;
}

void amper_UD_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct dhmp_UD_response* response_msg = msg->data;
	struct dhmp_UD_work * task = response_msg->req_info.task;
	if(response_msg->write_flag == 0)
	{
		void* temp = response_msg + sizeof(struct dhmp_UD_response);
		int i;
		for(i = 0;i<response_msg->batch;i++)
		{
			memcpy((void*)((response_msg->req_info.local_addr)[i]), temp , response_msg->req_info.req_size);
			temp += response_msg->req_info.req_size;
		}
	}
	task->recv_flag = true;
	INFO_LOG("UD flush ");
}

void amper_herd_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	void* response = msg->data;
	char write_flag = *(char*)response;
	char cur = *(char*)(response+1);
	
	if(write_flag == 0)
	{
		void* local_addr = (void *)*(uintptr_t*)(response + 2 + sizeof(uintptr_t));
		size_t size = *(char*)(response + 2 + sizeof(uintptr_t)*2);
		response = (response + 2 + sizeof(uintptr_t)*2 + sizeof(size_t));
		memcpy(local_addr, response , size);
	}

	pthread_mutex_lock(&client->mutex_request_num);
	client->herd.is_available[cur] = 1;
	pthread_mutex_unlock(&client->mutex_request_num);

	INFO_LOG("herd flush ");
}

void amper_pRDMA_WS_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	void* response = msg->data;
	char write_flag = *(char*)response;
	char cur = *(char*)(response+1);
	
	if(write_flag == 0)
	{
		void* local_addr = (void *)*(uintptr_t*)(response + 2 + sizeof(uintptr_t));
		size_t size = *(char*)(response + 2 + sizeof(uintptr_t)*2);
		response = (response + 2 + sizeof(uintptr_t)*2 + sizeof(size_t));
		memcpy(local_addr, response , size);
	}

	pthread_mutex_lock(&client->mutex_request_num);
	client->pRDMA.is_available[cur].value = 1;
	// free(client->pRDMA.is_available[cur].task);??????
	
	pthread_mutex_unlock(&client->mutex_request_num);

	INFO_LOG("pRDMA request over ");
}

void dhmp_octo_request_handler(struct dhmp_transport* rdma_trans)
{
	struct dhmp_octo_request * request;
	void *response ,*cur_addr;
	cur_addr = server->octo[rdma_trans->node_id].S_mr->addr;
	request = cur_addr;
	size_t reply_size = sizeof(struct dhmp_octo_request);
	
	char * context_ptr =  cur_addr + sizeof(struct dhmp_octo_request);
	void* server_addr = (*request).server_addr;
	size_t size = (*request).req_size;	
	char write_flag = (*request).flag_write;


	if(write_flag == 0)
		reply_size = reply_size + size;
	response = server->octo[rdma_trans->node_id].local_mr->addr;
	memcpy (response , server->octo[rdma_trans->node_id].S_mr , sizeof(struct dhmp_octo_request));
	if(write_flag == 1)
	{
		memcpy(server_addr, context_ptr, size);
		_mm_clflush(server_addr);
	}
	else
	{
		response += sizeof(struct dhmp_octo_request);
		memcpy(response, server_addr ,size);
	}

	struct dhmp_task* write_task = dhmp_write_task_create(rdma_trans, NULL, 0);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return ;
	}

	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr= ( uintptr_t ) server->octo[rdma_trans->node_id].C_mr.addr;
	send_wr.wr.rdma.rkey= server->octo[rdma_trans->node_id].C_mr.rkey;
	send_wr.imm_data = NUM_octo_res;
	sge.addr= ( uintptr_t ) server->octo[rdma_trans->node_id].local_mr->addr;
	sge.length= reply_size;
	sge.lkey= server->octo[rdma_trans->node_id].local_mr->lkey;
	ibv_post_send (rdma_trans->qp, &send_wr, &bad_wr );
	while(!write_task->done_flag);
	return ;
}

void dhmp_octo_response_handler(struct dhmp_transport* rdma_trans) // read
{
	struct dhmp_octo_request * request;
	void *cur_addr;
	cur_addr = client->octo.C_mr->addr;
	request = cur_addr;

	char * context_ptr =  cur_addr + sizeof(struct dhmp_octo_request);
	void* server_addr = (*request).server_addr;
	void* client_addr = (*request).client_addr;
	size_t size = (*request).req_size;	
	char write_flag = (*request).flag_write;
	if(write_flag == 0)
	{
		memcpy(client_addr, context_ptr, size);
		_mm_clflush(client_addr);
	}
	struct amper_L5_work *work = (*request).task;
	work->done_flag = true;
}
