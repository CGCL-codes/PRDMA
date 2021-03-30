#ifndef DHMP_TRANSPORT_H
#define DHMP_TRANSPORT_H

#define ADDR_RESOLVE_TIMEOUT 500
#define ROUTE_RESOLVE_TIMEOUT 500

#define RECV_REGION_SIZE (512*1024*1024)
#define SEND_REGION_SIZE (512*1024*1024)

/*recv region include poll recv region,normal recv region*/
#define SINGLE_POLL_RECV_REGION (64*1024*1024)
#define SINGLE_NORM_RECV_REGION (1*1024*1024)

enum dhmp_transport_state {
	DHMP_TRANSPORT_STATE_INIT,
	DHMP_TRANSPORT_STATE_LISTEN,
	DHMP_TRANSPORT_STATE_CONNECTING,
	DHMP_TRANSPORT_STATE_CONNECTED,
	DHMP_TRANSPORT_STATE_DISCONNECTED,
	DHMP_TRANSPORT_STATE_RECONNECT,
	DHMP_TRANSPORT_STATE_CLOSED,
	DHMP_TRANSPORT_STATE_DESTROYED,
	DHMP_TRANSPORT_STATE_ERROR
};

struct dhmp_cq{
	struct ibv_cq	*cq;
	struct ibv_comp_channel	*comp_channel;
	struct dhmp_device *device;
	
	/*add the fd of comp_channel into the ctx*/
	struct dhmp_context *ctx;
};

struct dhmp_mr{
	size_t cur_pos;
	void *addr;
	struct ibv_mr *mr;
};

struct dhmp_transport{
	struct sockaddr_in	peer_addr;
	struct sockaddr_in	local_addr;
	
	int node_id;
	enum dhmp_transport_state trans_state;
	struct dhmp_context *ctx;
	struct dhmp_device *device;
	struct dhmp_cq *dcq;
	struct ibv_qp *qp;
	struct rdma_event_channel *event_channel;
	struct rdma_cm_id	*cm_id;

	/*the var use for two sided RDMA*/
	struct dhmp_mr send_mr;
	struct dhmp_mr recv_mr;
	size_t send_recv_size;
	
	bool is_poll_qp;

	long dram_used_size;
	long nvm_used_size;
	
	struct list_head client_entry;

	struct ibv_ah * ah;
	uint32_t qp_num;
	uint32_t qkey;
};

struct dhmp_send_mr{
	struct ibv_mr *mr;
};

struct dhmp_transport* dhmp_transport_create(struct dhmp_context* ctx, 
													struct dhmp_device* dev,
													bool is_listen,
													bool is_poll_qp);

void dhmp_transport_destroy(struct dhmp_transport *rdma_trans);


int dhmp_transport_connect(struct dhmp_transport* rdma_trans,
                             	const char* url, int port);

int dhmp_transport_listen(struct dhmp_transport* rdma_trans, int listen_port);

void amper_ud_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr);
struct dhmp_task * dhmp_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr);

struct dhmp_send_mr* dhmp_create_smr_per_ops(struct dhmp_transport* rdma_trans, void* addr, int length );

void dhmp_WriteImm3_request_handler(struct dhmp_transport* rdma_trans) ;
void dhmp_WriteImm3_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_ReqAddr1_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_ReqAddr1_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_WriteImm_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_WriteImm_request_handler(uint32_t task_offset);
void dhmp_send2_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_send2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_WriteImm2_request_handler(struct dhmp_transport* rdma_trans);
void dhmp_WriteImm2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_Write2_request_handler();
void dhmp_Write2_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_Sread_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_Sread_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void dhmp_Sread_server();



void amper_tailwindRPC_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void amper_tailwindRPC_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void amper_DaRPC_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void amper_DaRPC_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);

void amper_UD_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void amper_UD_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);

void amper_herd_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);
void amper_pRDMA_WS_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg);

void dhmp_octo_request_handler(struct dhmp_transport* rdma_trans);
void dhmp_octo_response_handler(struct dhmp_transport* rdma_trans);
#endif

