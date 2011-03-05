%%%-------------------------------------------------------------------
%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2011, Gianfranco
%%% Created : 14 Feb 2011 by Gianfranco <zenon@zen.local>
%%%-------------------------------------------------------------------
-module(wn_resource_process).
-behaviour(gen_server).
-include("include/worker_net.hrl").
-define(TIMEOUT,3000).
-record(state,{node_root :: string(),
	       queues :: [{atom,[pid()]}],
	       working, %% ets {pid(),pid(),atom()}
	       slots    %% ets {atom,non_neg_integer()|infinity}
	      }).

%% API
-export([start_link/2,signal/3,queued/1,
	 cancel/3
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(string(),[resource_spec()]) -> {ok,pid()} | {error,term()}).
start_link(NodeRoot,TypeSpec) ->
    gen_server:start_link(?MODULE, {NodeRoot,TypeSpec}, []).

-spec(signal(pid(),pid(),atom()) -> ok).
signal(Pid,JobKeeperPid,QueueType) ->    
    gen_server:cast(Pid,{signal,JobKeeperPid,QueueType}).

-spec(queued(pid()) -> [{atom(),[#wn_job{}]}]).
queued(Pid) ->
    gen_server:call(Pid,queued).

-spec(cancel(pid(),pid(),atom()) -> ok | {error,term()}).
cancel(Pid,JobKeeperPid,Type) ->
    gen_server:call(Pid,{cancel,JobKeeperPid,Type}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({NodeRoot,TypeSpec}) ->
    Slots = ets:new(available,[set]),
    Working = ets:new(working,[set]),
    lists:foreach(fun({Type,Amount}) ->
			  ets:insert(Slots,{Type,Amount}),
			  ets:insert(Working,{Type,[]})
		  end,TypeSpec),	      
    {ok,#state{node_root = NodeRoot,
	       queues = [{Type,[]} || {Type,_} <- TypeSpec],
	       slots = Slots,
	       working = Working
	      }}.

handle_call({cancel,JobKeeperPid,Type},_From,State) ->
    {value,{_,TypeQueue},Q} = lists:keytake(Type,1,State#state.queues),
    case TypeQueue -- [JobKeeperPid] of
	TypeQueue ->
	    {reply,{error,{not_in_queue,Type}},State};
	X ->
	    {reply,ok,State#state{queues = [{Type,X}|Q]}}
    end;

handle_call(queued, _From, State) ->
    Reply = collect_jobs(State),
    {reply, Reply, State}.

handle_cast({signal,JobKeeperPid,QueueType}, State) ->
    {noreply,
     case {ets:lookup(State#state.slots,QueueType),
	   lists:keytake(QueueType,1,State#state.queues)} of
	 {[{QueueType,infinity}], _ } ->
	     try_dispatch_job(JobKeeperPid,State,QueueType),
	     State;
	 {[{QueueType,Available}], {value,{_,[]},_}} when Available > 0 ->
	     case try_dispatch_job(JobKeeperPid,State,QueueType) of
		 ok -> ets:insert(State#state.slots,{QueueType,Available-1});
		 {error,taken} -> ignore
	     end,
	     State;
	 {[{QueueType,_}], {value,{Type,Queue},Queues}} ->
	     State#state{queues = [{Type,Queue++[JobKeeperPid]}|Queues]}
     end}.

handle_info({'EXIT',WorkerPid,_}, State) ->
    {noreply,
     begin
	 [{WorkerPid,_,QueueType}] = ets:lookup(State#state.working,WorkerPid),
	 true = ets:delete(State#state.working,WorkerPid),

         case lists:keytake(QueueType,1,State#state.queues) of
	     {value,{_,[]},_} ->
		 case ets:lookup(State#state.slots,QueueType) of
		     [{QueueType,infinity}] -> ignore;
		     [{QueueType,X}] -> ets:insert(State#state.slots,{QueueType,X+1})
		 end,
		 State;
	     {value,{Type,[QueuedPid|R]},Queues} ->	     
		 case try_dispatch_job(QueuedPid,State,QueueType) of
		     ok ->
			 State#state{queues = [{Type,R}|Queues]};
		 {error,taken} -> 
			 State
		 end
	 end
     end}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
try_dispatch_job(JobKeeperPid,State,QueueType) ->
    case wn_job_keeper:signal(JobKeeperPid) of
	{ok,WnJob} ->
	    process_flag(trap_exit,true),
	    {ok,WorkerPid}  = wn_job_worker:start_link(State#state.node_root,
						       JobKeeperPid,WnJob),
	    ets:insert(State#state.working,{WorkerPid,JobKeeperPid,QueueType}),
	    ok;
	{error,taken} ->
	    {error,taken}
    end.

collect_jobs(State) ->
    [{Type,[ wn_job_keeper:get_job(Pid) || Pid <- Queue]}
     || {Type,Queue} <- State#state.queues].
