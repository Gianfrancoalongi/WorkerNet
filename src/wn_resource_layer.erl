%%%-------------------------------------------------------------------
%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2010, Gianfranco
%%% Created : 11 Dec 2010 by Gianfranco <zenon@zen.local>
%%%-------------------------------------------------------------------
-module(wn_resource_layer).
-behaviour(gen_server).
-include("include/worker_net.hrl").

%% API
-export([start_link/1,
	 register/1,list_resources/0,stop/0,
	 deregister/2,queued/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state,
	{resources, %% ETS {Name,#wn_resource{}}
	 node_root :: string(),
	 parent_pid :: pid()
	}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(string()) -> {ok,pid()} | term()).	     
start_link(NodeRoot) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, NodeRoot, []).

-spec(register(#wn_resource{}) -> ok | {error,term()}).
register(Resource) ->
    gen_server:call(?MODULE,{register,Resource}).

-spec(deregister(node(),string()) -> ok | {error,term()}).	     
deregister(Node,Name) ->
    gen_server:call(?MODULE,{deregister,Node,Name}).    

-spec(list_resources() -> [#wn_resource{}]).	     
list_resources() ->
    gen_server:call(?MODULE,list_all_resources).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?MODULE,stop).

-spec(queued(node(),string()) -> {ok,[{atom(),[#wn_job{}]}]} | {error,term()}).
queued(Node,Name) ->
    gen_server:call(?MODULE,{queued,Node,Name}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(NodeRoot) ->
    {ok, #state{resources = ets:new(resources,[set]),
		node_root = NodeRoot
	       }}.

handle_call(stop,_From,State) ->
    {stop,normal,ok,State};

handle_call({queued,Node,Name},From,State) ->
    case {Node == node(), lists:member(Node,nodes())} of
	{true,_} ->
	    Reply = try_get_queued(State,Name),
	    {reply,Reply,State};
	{false,true} ->
	    gen_server:cast({?MODULE,Node},{queued,From,Name}),
	    {noreply,State};
	  {false,false} ->
	    {reply,{error,noresides},State}
    end;

handle_call(list_all_resources,From,State) ->
    spawn(resource_collector(From)),
    {noreply,State};

handle_call(list_resources,_From,State) ->
    {reply,[V || {_,V} <- ets:tab2list(State#state.resources)],State};

handle_call({register,Resource},From, State) ->
    #wn_resource{resides=Node} = Resource,
    case {Node == node(),lists:member(Node,nodes())} of
	{true,_} ->
	    Reply = try_register(State,Resource),
	    {reply,Reply,State};
	{false,true} ->
	    gen_server:cast({?MODULE,Node},{register,From,Resource}),
	    {noreply,State};
	{false,false} ->
	    {reply,{error,noresides},State}
    end;

handle_call({deregister,Node,Name},From,State) ->
    case {Node == node(),lists:member(Node,nodes())} of
	{true,_} ->
	    Reply = try_deregister(State,Name),
	    {reply,Reply,State};
	{false,true} ->	    
	    gen_server:cast({?MODULE,Node},{deregister,From,Node,Name}),
	    {noreply,State};
	{false,false} ->
	    {reply,{error,noresides},State}
    end.

handle_cast({queued,From,Name},State) ->
    gen_server:reply(From,try_get_queued(State,Name)),
    {noreply, State};

handle_cast({deregister,From,_Node,Name},State) ->
    gen_server:reply(From,try_deregister(State,Name)),
    {noreply, State};

handle_cast({register,From,Resource},State) ->
    gen_server:reply(From,try_register(State,Resource)),
    {noreply, State}.

handle_info(_Msg,State) ->
    {noreply,State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
try_get_queued(State,Name) ->
    case ets:lookup(State#state.resources,Name) of
	[] -> {error,noexists};	    
	[{Name,WnResource}] ->
	    Pid = WnResource#wn_resource.pid,
	    {ok,wn_resource_process:queued(Pid)}
    end.

try_deregister(State,Name) ->
    case ets:lookup(State#state.resources,Name) of
	[] -> {error,noexists};
	[{Name,WnResource}] ->
	    exit(WnResource#wn_resource.pid,deregistered),
	    ets:delete(State#state.resources,Name),
	    ok
    end.

try_register(State,Resource) ->
    #wn_resource{name=Name} = Resource,
    case ets:lookup(State#state.resources,Name) of
	[] ->
	    process_flag(trap_exit,true),
	    {ok,Pid} = wn_resource_process:start_link(State#state.node_root,
						      Resource#wn_resource.type),
	    ets:insert(State#state.resources,
		       {Name,Resource#wn_resource{pid=Pid}}),
	    ok;
	_ ->
	    {error,already_exists}
    end.

resource_collector(From) ->
    Nodes = [node()|nodes()],
    fun() ->
	    Res =
		lists:foldr(
		  fun(Node,Acc) ->
			  case rpc:call(Node,erlang,whereis,[?MODULE]) of
			      undefined -> Acc;
			      _Pid ->
				  gen_server:call({?MODULE,Node},
						  list_resources)++Acc
			end
		  end,[],Nodes),
	    gen_server:reply(From,Res)
    end.

    
