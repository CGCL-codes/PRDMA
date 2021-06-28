#ifndef DHMP_SERVER_H
#define DHMP_SERVER_H
/*decide the buddy system's order*/
#define MAX_ORDER 5
#define SINGLE_AREA_SIZE 4194304  



struct dhmp_free_block{
	void *addr;
	size_t size;
	struct ibv_mr *mr;
	struct list_head free_block_entry;
};

struct dhmp_free_block_head{
	struct list_head free_block_list;
	int nr_free;
};

struct dhmp_area{
	//pthread_spinlock_t mutex_area;
	struct ibv_mr *mr;
	size_t size;
	struct list_head area_entry;
};
struct dhmp_tasklist{
	struct dhmp_transport *rdma_trans;
	size_t length;
	void * dhmp_addr;
	bool * cmpflag;
};

struct dhmp_server{
	struct dhmp_context ctx;
	struct dhmp_config config;

	struct dhmp_transport *listen_trans;
	struct dhmp_transport *connect_trans[DHMP_CLIENT_NODE_NUM];
	
	struct list_head dev_list;
	pthread_mutex_t mutex_client_list;
	struct list_head client_list;

	/*below structure about area*/
	struct list_head area_list[DHMP_CLIENT_NODE_NUM];

	int cur_connections;
	long nvm_used_size;
	long nvm_total_size;
	
	void * ringbufferAddr;
	uint64_t cur_addr;
	struct dhmp_transport* rdma_trans;
	struct dhmp_tasklist tasklist[30];
	pthread_t model_B_write_epoll_thread;
	pthread_t model_C_Sread_epoll_thread;
	bool model_C_new_msg;

	int client_num;

	struct{
		struct ibv_mr C_mr;
		struct ibv_mr* local_mr;
		struct ibv_mr* S_mr;
	}octo[DHMP_CLIENT_NODE_NUM];

	struct  {
		struct ibv_mr* mr;
		void * addr;
		struct ibv_mr* read_mr;
	} L5_mailbox;
	struct  {
		struct ibv_mr* mr;
		void * addr;
		struct ibv_mr C_mr;
		struct dhmp_send_mr* reply_smr;
		bool is_new;
	} L5_message[DHMP_CLIENT_NODE_NUM]; 
	pthread_t L5_poll_thread;
	pthread_t L5_flush2_poll_thread[DHMP_CLIENT_NODE_NUM];

	struct  {
		struct ibv_mr* mr;
		void * addr;
		struct ibv_mr flush2_mr;
		pthread_t poll_thread;
	} Tailwind_buffer[DHMP_CLIENT_NODE_NUM];

	struct  {
		struct ibv_mr* mr;
		void * addr;
	} herd_buffer[DHMP_CLIENT_NODE_NUM];

	struct ibv_mr* read_mr;
	struct dhmp_cq* DaRPC_dcq[DaRPC_clust_NUM];
	char DaRPC_dcq_count[DaRPC_clust_NUM];

	struct{
		struct ibv_mr* write_mr;
		struct ibv_mr* read_mr;
		pthread_t poll_thread;
		char time;
		size_t size;
	}RFP[DHMP_CLIENT_NODE_NUM];

	struct{
		struct ibv_mr* Sreq_mr;
		struct ibv_mr* Sdata_mr;
		struct ibv_mr Cdata_mr;
		struct ibv_mr* Slocal_mr;
		size_t size;
	}scaleRPC[DHMP_CLIENT_NODE_NUM];
	pthread_t scalable_poll_thread[DHMP_CLIENT_NODE_NUM];

	struct{
		struct ibv_mr C_mr;
		struct ibv_mr* S_mr;
		pthread_t poll_thread;
		size_t size;
		struct ibv_mr* local_mr;
	}FaRM[DHMP_CLIENT_NODE_NUM];

	struct{
		struct ibv_mr C_mr;
		struct ibv_mr* S_mr;
		pthread_t poll_thread;
		size_t size;
	}herd[DHMP_CLIENT_NODE_NUM];

	struct{
		struct ibv_mr* S_mr;
		pthread_t poll_thread;
		size_t size;
	}pRDMA[DHMP_CLIENT_NODE_NUM];
};

extern struct dhmp_server *server;

struct dhmp_area *dhmp_area_create(size_t length);
struct ibv_mr * dhmp_malloc_poll_area(size_t length);

struct dhmp_device *dhmp_get_dev_from_server();

void amper_allocspace_for_server(struct dhmp_transport* rdma_trans, int is_special, size_t size);
//#1 for RFP
#endif


