package rgo

import ()

type RData struct {
	bucket      string
	key         string
	value       interface{}
	contentType string
	vclock      string
	meta        interface{}
	links       []string
	siblings    []string
}

type Data struct {
	key   string
	value string
}

type Resources struct {
	Riak_kv_wm_buckets     string
	Riak_kv_wm_index       string
	Riak_kv_wm_link_walker string
	Riak_kv_wm_mapred      string
	Riak_kv_wm_object      string
	Riak_kv_wm_ping        string
	Riak_kv_wm_props       string
	Riak_kv_wm_stats       string
	Riak_solr_searcher_wm  string
	Riak_solr_indexer_wm   string
}

type Status struct {
	Vnode_gets               int
	Vnode_puts               int
	Read_repairs             int
	Vnode_gets_total         int
	Vnode_puts_total         int
	Node_gets                int
	Node_gets_total          int
	Node_get_fsm_time_mean   interface{}
	Node_get_fsm_time_median interface{}
	Node_get_fsm_time_95     interface{}
	Node_get_fsm_time_99     interface{}
	Node_get_fsm_time_100    interface{}
	Node_puts                int
	Node_puts_total          int
	Node_put_fsm_time_mean   interface{}
	Node_put_fsm_time_median interface{}
	Node_put_fsm_time_95     interface{}
	Node_put_fsm_time_99     interface{}
	Node_put_fsm_time_100    interface{}
	Read_repairs_total       int
	Cpu_nprocs               int
	Cpu_avg1                 int
	Cpu_avg5                 int
	Cpu_avg15                int
	Mem_total                int
	Mem_allocated            int
	Nodename                 string
	Connected_nodes          []string
	Sys_driver_version       string
	Sys_global_heaps_size    int
	Sys_heap_type            string
	Sys_logical_processors   int
	Sys_otp_release          string
	Sys_process_count        int
	Sys_smp_support          bool
	Sys_system_version       string
	Sys_system_architecture  string
	Sys_threads_enabled      bool
	Sys_thread_pool_size     int
	Sys_wordsize             int
	Ring_members             []string
	Ring_num_partitions      int
	Ring_ownership           string
	Ring_creation_size       int
	Storage_backend          string
	Pbc_connects_total       int
	Pbc_connects             int
	Pbc_active               int
	Riak_kv_version          string
	Riak_core_version        string
	Bitcask_version          string
	Luke_version             string
	Webmachine_version       string
	Mochiweb_version         string
	Erlang_js_version        string
	Runtime_tools_version    string
	Crypto_version           string
	Os_mon_version           string
	Sasl_version             string
	Stdlib_version           string
	Kernel_version           string
}
