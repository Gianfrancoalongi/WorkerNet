%%%-------------------------------------------------------------------
%%% @author Gianfranco <zenon@zen.home>
%%% @copyright (C) 2011, Gianfranco
%%% Created :  4 Jan 2011 by Gianfranco <zenon@zen.home>
%%%-------------------------------------------------------------------
-module(wn_job_layer).
-behaviour(gen_server).
-include("include/worker_net.hrl").

%% API
-export([start_link/0,register/1,list_all_jobs/0,
	 stop/0,stream/2,result/1,finished_at/1,
	 cancel/1,stored_result/1,delete/1
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {jobs % ets table {Name,Pid,WnJob}
	       }).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link() -> {ok,pid()} | {error,term()}).	     
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(register(#wn_job{}) -> ok | {error,term()}).	     
register(WnJob) ->
    case try_send_files(WnJob#wn_job.files) of
	ok -> gen_server:call(?MODULE,{add_job,WnJob});
	E -> E
    end.

-spec(list_all_jobs() -> [#wn_job{}]).
list_all_jobs() ->
    gen_server:call(?MODULE,list_all_jobs).

-spec(stop() -> ok).	     
stop() ->
    gen_server:call(?MODULE,stop).

-spec(stream(term(),string()) -> ok | {error,term()}).
stream(IoDev,Id) ->
    Stream = fun(F) -> receive {T,E} -> io:format(IoDev,"~p : ~p~n",[T,E]) end, F(F) end,
    StreamPid = spawn_link(fun() -> Stream(Stream) end),
    case gen_server:call(?MODULE,{stream,StreamPid,Id}) of
	ok -> ok;	    
	Err ->
	    exit(StreamPid,Err),
	    Err
    end.

-spec(result(string()) -> {ok,[string()]} | {error,term()}).    
result(Id) ->
    gen_server:call(?MODULE,{result,Id}).

-spec(finished_at(string()) -> {ok,time_marker()} | {error,term()}).
finished_at(Id) ->
    gen_server:call(?MODULE,{finished_at,Id}).

-spec(cancel(string()) -> ok | {error,term()}).
cancel(Id) ->
    gen_server:call(?MODULE,{cancel,Id}).

-spec(stored_result(string()) -> {ok,string()} | {error,term()}).             
stored_result(Id) ->
    gen_server:call(?MODULE,{stored_result,Id}).

-spec(delete(string()) -> ok | {error,term()}).             
delete(Id) ->
    gen_server:call(?MODULE,{delete,Id}).
    

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {ok, #state{jobs = ets:new(jobs_table,[set])}}.

handle_cast(_Msg,State) ->
    {noreply,State}.

handle_call({delete,Id},_,State) ->
    Result = try_delete(Id,State),
    {reply,Result,State};

handle_call({stored_result,Id},_,State) ->
    Result = try_get_stored(Id,State),
    {reply,Result,State};

handle_call({cancel,Id},_,State) ->
    Reply = try_cancel(Id,State),
    {reply,Reply,State};
    
handle_call(stop,_From,State) ->
    {stop,normal,ok,State};

handle_call({finished_at,Id},_From,State) ->
    {reply,
     case ets:lookup(State#state.jobs,Id) of
	 [] -> {error,no_such_job};
	 [{Id,Pid,_}] ->
	     wn_job_keeper:get_done(Pid)
     end,State};

handle_call({result,Id},_From,State) ->
    {reply,
     case ets:lookup(State#state.jobs,Id) of
	 [] -> {error,no_such_job};
	 [{Id,Pid,_}] ->
	     {ok,wn_job_keeper:get_result(Pid)}
     end, State};

handle_call({stream,StreamPid,Id},_From,State) ->
    {reply,
     case ets:lookup(State#state.jobs,Id) of
	 [] -> {error,no_such_job};
	 [{Id,Pid,_}] ->
	     wn_job_keeper:stream(Pid,StreamPid),
	     ok
     end,
     State};

handle_call(list_all_jobs,From,State) ->
    spawn_link(job_collector(From)),
    {noreply,State};

handle_call(list_jobs,_From,State) ->
    {reply,[WnJob || {_,_,WnJob} <- ets:tab2list(State#state.jobs)],State};

handle_call({add_job,WnJob}, _From, State) ->
    JobId = WnJob#wn_job.id,
    {reply,
     case ets:lookup(State#state.jobs,JobId) of
	 [] ->
	     {ok,Pid} = wn_job_keeper:start_link(WnJob),
	     ets:insert(State#state.jobs,{JobId,Pid,WnJob}),	       
	     lists:foreach(
	       fun(WnResource)  ->
		       case resource_is_sufficient(WnJob,WnResource) of
			   {true,Possibles} ->
			       lists:foreach(
				 fun(Type) ->
					 signal_resource(Pid,WnResource,Type)
				 end,Possibles);
			   false -> ignore
		       end
	       end,wn_resource_layer:list_resources()),
	     ok;
	 [_] ->
	     lists:foreach(
	       fun(File) ->
		       wn_file_layer:delete_file(File#wn_file.resides,
						 File#wn_file.id)
	       end,WnJob#wn_job.files),
	     {error,already_exists}
     end, State}.
    
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

try_delete(Id,State) ->
    case ets:lookup(State#state.jobs,Id) of
        [{Id,JobKeeperPid,_}] ->
            case wn_job_keeper:get_stored_result(JobKeeperPid) of
                {ok,Result} ->
                    ok = wn_job_keeper:delete(JobKeeperPid),
                    ets:delete(State#state.jobs,Id),
                    wn_file_layer:delete_file(node(),Result);
                X -> X
            end;
        [] ->
            {error,no_such_job}
    end.

try_get_stored(Id,State) ->
    case ets:lookup(State#state.jobs,Id) of
        [{Id,JobKeeperPid,_}] ->
            wn_job_keeper:get_stored_result(JobKeeperPid);
        [] -> {error,no_such_job}
    end.

try_cancel(Id, State) ->
    case ets:lookup(State#state.jobs,Id) of
	[{Id,JobKeeperPid,WnJob}] ->
	    ets:delete(State#state.jobs,Id),
	    wn_job_keeper:cancel(JobKeeperPid),
            cancel_resources(JobKeeperPid,WnJob),
    	    ok;
	[] ->
	    {error,no_such_job}
    end.

cancel_resources(JobKeeperPid,WnJob) ->
    lists:foreach(
      fun(WnResource) ->
	      case resource_is_sufficient(WnJob,WnResource) of
		  {true,Possibles} ->
		      WnPid = WnResource#wn_resource.pid,
		      lists:foreach(
			fun(Type) ->
				wn_resource_process:cancel(WnPid,
							   JobKeeperPid,
							   Type)
			end,
			Possibles);
		  false -> ignore
	      end
      end,
      wn_resource_layer:list_resources()).

try_send_files([F|R]) ->
    case wn_file_layer:add_file(F) of
	ok -> try_send_files(R);
	E -> E
    end;
try_send_files([]) -> ok.

resource_is_sufficient(WnJob,WnResource) ->
    case [ T || {T,_} <- WnResource#wn_resource.type,
		lists:member(T,WnJob#wn_job.resources)] of
	[] -> false;
	L -> {true,L}
    end.

signal_resource(JobKeeperPid,WnResource,Type) ->
    wn_resource_process:signal(WnResource#wn_resource.pid,
			       JobKeeperPid,Type).

job_collector(From) ->
    Nodes = [node()|nodes()],
    fun() ->
	    Res =
		lists:foldr(
		  fun(Node,Acc) ->
			  case rpc:call(Node,erlang,whereis,[?MODULE]) of
			      undefined -> Acc;
			      _Pid ->
				  gen_server:call({?MODULE,Node},
						  list_jobs)++Acc
			  end
		  end,[],Nodes),
	    gen_server:reply(From,Res)
    end.
