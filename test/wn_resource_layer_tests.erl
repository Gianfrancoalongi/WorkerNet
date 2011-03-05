%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2010, Gianfranco
%%% Created : 10 Dec 2010 by Gianfranco <zenon@zen.local>
-module(wn_resource_layer_tests). 
-include_lib("eunit/include/eunit.hrl").
-include("include/worker_net.hrl").
-define(NODE_ROOT,
	"/Users/zenon/ErlangBlog/worker_net-0.1/node_root/").

local_resource_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Can register resources locally",fun register_locally/0}
     ]}.

distr_resource_test_() ->
    {foreach,
     fun distr_setup/0,
     fun distr_cleanup/1,
     [
      fun register_distributed/1,
      fun register_restart_register/1,
      fun register_deregister/1
     ]
    }.

        
register_locally() ->
    ResourceA = #wn_resource{name = "macbook pro laptop",
			     type = [{'os-x',1},{bsd,1}],
			     resides = node()},
    ResourceB = #wn_resource{name = "erlang runtime system",
			     type = [{erlang,4}],
			     resides = node()},
    ok = wn_resource_layer:register(ResourceA),
    ok = wn_resource_layer:register(ResourceB),
    List = lists:sort(wn_resource_layer:list_resources()),
    resource_processes_are_alive([ResourceA,ResourceB],List).

register_distributed([N1,N2]) ->
    {"Can Register Distributed",
     fun() ->
	     ResourceA = #wn_resource{name = "erlang R14",
				      type = [{erlang,infinity}],
				      resides = N1},
	     ResourceB = #wn_resource{name = "os-x macbook pro",
				      type = [{'os-x',1}],
				      resides = N2},
	     ResourceC = #wn_resource{name = "g++",
				      type = [{'g++',1}],
				      resides = node()},
	     ok = wn_resource_layer:register(ResourceA),
	     ok = wn_resource_layer:register(ResourceB),
	     ok = wn_resource_layer:register(ResourceC),
	     ListA = lists:sort(wn_resource_layer:list_resources()),
	     ListB = lists:sort(rpc:call(N1,wn_resource_layer,list_resources,[])),
	     ListC = lists:sort(rpc:call(N2,wn_resource_layer,list_resources,[])),
	     ?assertEqual(3,length(ListA)),
	     ?assertEqual(ListB,ListA),
	     ?assertEqual(ListB,ListC),
	     resource_processes_are_alive([ResourceA,ResourceB,ResourceC],ListA)
     end}.

register_restart_register([N1,N2]) ->
    {"Can Register, Restart and Register",
     fun() ->
	     ResourceA = #wn_resource{name = "erlang R14",
				      type = [{erlang,infinity}],
				      resides = N1},
	     ResourceB = #wn_resource{name = "os-x macbook pro",
				      type = [{'os-x',1}],
				      resides = N2},
	     ResourceC = #wn_resource{name = "g++",
				      type = [{'g++',1}],
				      resides = node()},
	     ok = wn_resource_layer:register(ResourceA),
	     ok = wn_resource_layer:register(ResourceB),
	     ok = wn_resource_layer:register(ResourceC),
	     M = fun() -> lists:sort(wn_resource_layer:list_resources()) end,
	     S1 = fun() -> lists:sort(rpc:call(N1,wn_resource_layer,list_resources,[])) end,
	     S2 = fun() -> lists:sort(rpc:call(N2,wn_resource_layer,list_resources,[])) end,
	     ?assertEqual(3,length(M())),
	     ?assertEqual(M(),S1()),
	     ?assertEqual(S1(),S2()),
	     resource_processes_are_alive([ResourceA,ResourceB,ResourceC],M()),
	     rpc:call(N1,wn_resource_layer,stop,[]),
	     ?assertEqual(M(),S2()),
	     resource_processes_are_alive([ResourceB,ResourceC],M()),	     
	     rpc:call(N2,wn_resource_layer,stop,[]),
	     resource_processes_are_alive([ResourceC],M()),	     
	     {ok,_} = rpc:call(N1,wn_resource_layer,start_link,[?NODE_ROOT]),
	     {ok,_} = rpc:call(N2,wn_resource_layer,start_link,[?NODE_ROOT]),
	     ok = wn_resource_layer:register(ResourceA),
	     resource_processes_are_alive([ResourceA,ResourceC],M()),
	     ok = wn_resource_layer:register(ResourceB),	     
	     resource_processes_are_alive([ResourceA,ResourceB,ResourceC],M())
     end}.

register_deregister([N1,N2]) ->
    {"Can Register, Deregister and Register",
     fun() ->
	     M = fun() -> lists:sort(wn_resource_layer:list_resources()) end,
	     S1 = fun() -> lists:sort(rpc:call(N1,wn_resource_layer,list_resources,[])) end,
	     S2 = fun() -> lists:sort(rpc:call(N2,wn_resource_layer,list_resources,[])) end,
	     resource_processes_are_alive([],S1()),	     
	     ResourceA = #wn_resource{name = "A",type = [{a,1}],resides = N1},
	     ResourceB = #wn_resource{name = "B",type = [{b,2}],resides = N2},
	     ResourceC = #wn_resource{name = "C",type = [{c,3}],resides = node()},
	     ok = wn_resource_layer:register(ResourceA),
	     ok = wn_resource_layer:register(ResourceB),
	     ok = wn_resource_layer:register(ResourceC),
	     resource_processes_are_alive([ResourceA,ResourceB,ResourceC],M()),
	     ?assertEqual(ok,wn_resource_layer:deregister(N1,"A")),
	     resource_processes_are_alive([ResourceB,ResourceC],S1()),
	     ?assertEqual(ok,wn_resource_layer:deregister(N2,"B")),
	     resource_processes_are_alive([ResourceC],S2()),
	     ?assertEqual(ok,wn_resource_layer:deregister(node(),"C")),
	     ?assertEqual([],M()),
	     ?assertEqual([],S1()),
	     ?assertEqual([],S2()),
	     ok = wn_resource_layer:register(ResourceA),
	     ok = wn_resource_layer:register(ResourceB),
	     ok = wn_resource_layer:register(ResourceC),
	     resource_processes_are_alive([ResourceA,ResourceB,ResourceC],M())
     end}.

%% -----------------------------------------------------------------
resource_processes_are_alive([],_) -> ok;    
resource_processes_are_alive([Expected|Tail],List) ->
    #wn_resource{name = Name, type = Type, resides = Resides} = Expected,
    Filtered = 
	lists:filter(
	   fun(#wn_resource{name=N,type=T,resides=R}) ->
		   N == Name andalso T == Type andalso R == Resides
	   end,List),
    ?assertMatch([_X],Filtered),
    [T] = Filtered,
    ?assertEqual(true,rpc:call(node(T#wn_resource.pid),erlang,is_process_alive,
			       [T#wn_resource.pid])),
    resource_processes_are_alive(Tail,List).	    
    
setup() ->
    {ok,_} = net_kernel:start([eunit_resource,shortnames]),
    erlang:set_cookie(node(),eunit),
    {ok,_} = wn_resource_layer:start_link(?NODE_ROOT).

cleanup(_) ->
    ok = net_kernel:stop(),
    ok = wn_resource_layer:stop().

distr_setup() ->
    setup(),
    Host = list_to_atom(inet_db:gethostname()),
    Args = " -pa "++hd(code:get_path())++" -setcookie eunit",
    {ok,N1} = slave:start(Host,n1,Args),
    {ok,N2} = slave:start(Host,n2,Args),    
    true = rpc:call(N1,net_kernel,connect_node,[N2]),
    {ok,_} = rpc:call(N1,wn_resource_layer,start_link,[?NODE_ROOT]),
    {ok,_} = rpc:call(N2,wn_resource_layer,start_link,[?NODE_ROOT]),
    [N1,N2].

distr_cleanup([N1,N2]) ->
    rpc:call(N1,wn_resource_layer,stop,[]),
    rpc:call(N2,wn_resource_layer,stop,[]),
    slave:stop(N1),
    slave:stop(N2),
    cleanup(nothing).
