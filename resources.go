package rgo

// Special structure to enumerate the resources returned from Client.ListResources
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
