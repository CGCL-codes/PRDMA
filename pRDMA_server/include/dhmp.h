#ifndef DHMP_H
#define DHMP_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/time.h>
#include <linux/list.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <numa.h>
#include "json-c/json.h"
#include <x86intrin.h> 

#define NUM_octo_req ((uint32_t)-4)
#define NUM_octo_res ((uint32_t)-5)



/* 
#define L5_MODEL L5_MODEL
#define RFP_MODEL RFP_MODEL
#define OCTOPUS_MODEL OCTOPUS_MODEL
#define SCALERPC_MODEL SCALERPC_MODEL
#define DARPC_MODEL DARPC_MODEL
#define FARM_MODEL FARM_MODEL
#define SRFLUSHRPC_MODEL SRFLUSHRPC_MODEL
#define SFLUSHRPC_MODEL SFLUSHRPC_MODEL
#define WRFLUSHRPC_MODEL WRFLUSHRPC_MODEL
#define WFLUSHRPC_MODEL WFLUSHRPC_MODEL
*/

//#define inject

#ifdef L5_MODEL
#define L5 L5 
#define FaRM_buffer_NUM 1 
#endif
#ifdef RFP_MODEL
#define RFP RFP
#define FaRM_buffer_NUM 1 
#endif
#ifdef OCTOPUS_MODEL
#define octopus octopus
#define FaRM_buffer_NUM 1 
#endif
#ifdef SCALERPC_MODEL
#define scaleRPC scaleRPC
#define FaRM_buffer_NUM 1 
#endif
#ifdef DARPC_MODEL
#define DaRPC DaRPC
#define FaRM_buffer_NUM 1 
#endif
#ifdef SRFLUSHRPC_MODEL
#define DaRPC DaRPC
#define RFLUSH RFLUSH
#define FaRM_buffer_NUM 4 
#endif
#ifdef FARM_MODEL
#define FaRM FaRM
#define FaRM_buffer_NUM 1 
#endif
#ifdef SFLUSHRPC_MODEL
#define DaRPC DaRPC
#define SFLUSH SFLUSH
#define FaRM_buffer_NUM 4 
#endif
#ifdef WRFLUSHRPC_MODEL
#define FaRM FaRM
#define RFLUSH RFLUSH
#define FaRM_buffer_NUM 4 
#endif
#ifdef WFLUSHRPC_MODEL
#define FaRM FaRM
#define WFLUSH WFLUSH
#define FaRM_buffer_NUM 4 
#endif

#define DHMP_CLIENT_NODE_NUM 100

// #define FaSST FaSST
// #define UD UD


#define NVM NVM

#define DaRPC_clust_NUM  5
#define BATCH 1
#define Tailwind_log_size 100


#define Sca1e_Swith_Time 100


#define max(a,b) (a>b?a:b)
#define min(a,b) (a>b?b:a)

#ifndef bool
#define bool char
#define true 1
#define false 0
#endif

enum dhmp_msg_type{
	DHMP_MSG_MALLOC_REQUEST,
	DHMP_MSG_MALLOC_RESPONSE,
	DHMP_MSG_MALLOC_ERROR,
	DHMP_MSG_FREE_REQUEST,
	DHMP_MSG_FREE_RESPONSE,
	DHMP_MSG_SERVER_INFO_REQUEST,
	DHMP_MSG_SERVER_INFO_RESPONSE,
	DHMP_MSG_CLOSE_CONNECTION,
	DHMP_MSG_SEND_REQUEST,
	DHMP_MSG_SEND_RESPONSE,
	DHMP_MSG_SEND2_REQUEST,
    DHMP_MSG_SEND2_RESPONSE,
	DHMP_MSG_REQADDR1_REQUEST,
	DHMP_MSG_REQADDR1_RESPONSE,
	DHMP_MSG_WriteImm_REQUEST,
	DHMP_MSG_WriteImm_RESPONSE,
	DHMP_MSG_WriteImm2_REQUEST,
	DHMP_MSG_WriteImm2_RESPONSE,
	DHMP_MSG_WriteImm3_REQUEST,
	DHMP_MSG_WriteImm3_RESPONSE,
	DHMP_MSG_Sread_REQUEST,
	DHMP_MSG_Sread_RESPONSE,
	DHMP_MSG_Write2_REQUEST,
	DHMP_MSG_Write2_RESPONSE,
	DHMP_MSG_Tailwind_RPC_requeset,
	DHMP_MSG_Tailwind_RPC_response,
	DHMP_MSG_DaRPC_requeset,
	DHMP_MSG_DaRPC_response,
	DHMP_MSG_UD_REQUEST,
	DHMP_MSG_UD_RESPONSE,
	DHMP_MSG_herd_RESPONSE,
	DHMP_MSG_pRDMA_WS_RESPONSE
};

/*struct dhmp_msg:use for passing control message*/
struct dhmp_msg{
	enum dhmp_msg_type msg_type;
	size_t data_size;
	void *data;
	bool is_next;
};

/*struct dhmp_addr_info is the addr struct in cluster*/
struct dhmp_addr_info{
	int read_cnt;
	int write_cnt;
	bool write_flag;
	struct ibv_mr nvm_mr;
	struct hlist_node addr_entry;
};

struct dhmp_server_addr_info{
	void* server_addr;
	struct hlist_node addr_entry;
};
/*dhmp malloc request msg*/
struct dhmp_mc_request{
	size_t req_size;
	struct dhmp_addr_info *addr_info;
	bool is_special;
	struct ibv_mr mr;
	struct ibv_mr mr2;
	void * task;
};

/*dhmp malloc response msg*/
struct dhmp_mc_response{
	struct dhmp_mc_request req_info;
	struct ibv_mr mr;
	struct ibv_mr mr2;
	struct ibv_mr read_mr;
	void * server_addr;
	void * server_addr2;
};

/*dhmp free memory request msg*/
struct dhmp_free_request{
	struct dhmp_addr_info *addr_info;
	struct ibv_mr mr;
void* dhmp_addr;
};

/*dhmp free memory response msg*/
struct dhmp_free_response{
	struct dhmp_addr_info *addr_info;
};


struct dhmp_Send_request{
	size_t req_size;
	void * dhmp_addr;
	void * task;
	void * local_addr;
	bool is_write;
};

struct dhmp_Send_response{
	struct dhmp_Send_request req_info;
};

struct dhmp_Send2_request{
	size_t req_size;
	void * server_addr;
	void * task;
};

struct dhmp_Send2_response{
	struct dhmp_Send2_request req_info;
};

struct dhmp_ReqAddr1_request{
	size_t req_size;
	void * dhmp_addr;
	void * task;
	bool * cmpflag;
};

struct dhmp_ReqAddr1_response{
	struct dhmp_ReqAddr1_request req_info;
	uint32_t task_offset;
};

struct dhmp_WriteImm_response
{
	bool * cmpflag;
};

struct dhmp_WriteImm2_request{
	enum dhmp_msg_type msg_type;
	size_t req_size;
	void * server_addr;
	void * task;
};

struct dhmp_WriteImm2_response{
	struct dhmp_WriteImm2_request req_info;
};

struct dhmp_WriteImm3_request{
	enum dhmp_msg_type msg_type;
	size_t req_size;
	void * dhmp_addr;
	void * task;
};

struct dhmp_WriteImm3_response{
	struct dhmp_WriteImm3_request req_info;
};

struct dhmp_Write2_request{
	bool is_new;
	size_t req_size;
	void * dhmp_addr;
	void * task;
};

struct dhmp_Write2_response{
	struct dhmp_Write2_request req_info;
};

struct dhmp_Sread_request{
	size_t req_size;
	void * task;
	struct ibv_mr client_mr;
	struct ibv_mr server_mr;
};

struct dhmp_Sread_response{
	struct dhmp_Sread_request req_info;
};


struct dhmp_TailwindRPC_request{
	size_t req_size;
	void * dhmp_addr;
	void * task;
};

struct dhmp_TailwindRPC_response{
	struct dhmp_TailwindRPC_request req_info;
};

struct dhmp_DaRPC_request{
	size_t req_size;
	uintptr_t * local_addr;
	void * task;
};

struct dhmp_DaRPC_response{
	struct dhmp_DaRPC_request req_info;
	int write_flag;
	int batch;
};


struct dhmp_UD_request{
	size_t req_size;
	uintptr_t * local_addr;
	void * task;
};

struct dhmp_UD_response{
	struct dhmp_UD_request req_info;
	int write_flag;
	int batch;
};

struct dhmp_octo_request{
	size_t req_size;
	void * server_addr;
	void* client_addr;
	void * task;
	char flag_write;
};


/**
 *	dhmp_malloc: remote alloc the size of length region
 */
void *dhmp_malloc(size_t length, int is_special);

void *dhmp_malloc_messagebuffer(size_t length, int is_special);

/**
 *	dhmp_read:read the data from dhmp_addr, write into the local_buf
 */
int dhmp_read(void *dhmp_addr, void * local_buf, size_t count);

/**
 *	dhmp_write:write the data in local buf into the dhmp_addr position
 */
int dhmp_write(void *dhmp_addr, void * local_buf, size_t count);

int amper_write_L5( void * local_buf, size_t count, void * globle_addr, char flag_write);
int amper_write_herd( void * local_buf, size_t count);
int amper_write_Tailwind(size_t offset, struct ibv_mr*head_mr, size_t head_size , void * local_buf, size_t size);

int amper_clover_compare_and_set(void *dhmp_addr, size_t length, uint64_t value);

/**
 *	dhmp_free:release remote memory
 */
void dhmp_free(void *dhmp_addr);

/**
 *	dhmp_client_init:init the dhmp client
 */
void dhmp_client_init(size_t size, int obj_num);

/**
 *	dhmp_client_destroy:clean RDMA resources
 */
void dhmp_client_destroy();

/**
 *	dhmp_server_init:init server 
 *	include config.xml read, rdma listen,
 *	register memory, context run. 
 */
void dhmp_server_init();

/**
 *	dhmp_server_destroy: close the context,
 *	clean the rdma resource
 */
void dhmp_server_destroy();
int dhmp_send(void *dhmp_addr, void * local_buf, size_t count, bool is_write);

void * nvm_malloc(size_t size);
void nvm_free(void *addr, size_t size);

void model_A_write(void * globle_addr, size_t length, void * local_addr);
void model_A_writeImm(void * globle_addr, size_t length, void * local_addr);
void model_B_write(void * globle_addr, size_t length, void * local_addr);
void model_B_writeImm(void * globle_addr, size_t length, void * local_addr);
void model_B_send(void * globle_addr, size_t length, void * local_addr);
void model_C_sread(void * globle_addr, size_t length, void * local_addr);
void model_D_write(void * server_addr, size_t length, void * local_addr);
void model_D_writeImm(void * server_addr, size_t length, void * local_addr);
void model_D_send(void * server_addr, size_t length, void * local_addr);

void model_1_octopus(void * globle_addr, size_t length, void * local_addr);
void model_1_octopus_R(void * globle_addr, size_t length, void * local_addr);
void model_1_clover(void * space_addr, size_t length, void * local_addr, uintptr_t* point_addr,int offset);
void model_1_clover_R(size_t length, void * local_addr, void* dhmp_addr);
void model_4_RFP( size_t length, void * local_addr, uintptr_t globle_addr, char flag_write);
void model_5_L5( size_t length, void * local_addr, void*  globle_addr, char flag_write);
void model_6_Tailwind(int accessnum, int obj_num , size_t length, void * local_addr);
void model_3_DaRPC( uintptr_t * globle_addr , size_t length, uintptr_t * local_addr, char flag_write, char batch);
void model_7_scalable( uintptr_t * globle_addr , size_t length, uintptr_t * local_addr, char flag_write, char batch);

void model_FaRM( void * local_buf, size_t count, void * globle_addr, char flag_write);

void send_UD(uintptr_t * globle_addr , size_t length, uintptr_t * local_addr, char flag_write, char batch);
#endif
