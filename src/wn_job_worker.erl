%%%-------------------------------------------------------------------
%%% @author Gianfranco <zenon@zen.home>
%%% @copyright (C) 2011, Gianfranco
%%% Created : 20 Jan 2011 by Gianfranco <zenon@zen.home>
%%%-------------------------------------------------------------------
-module(wn_job_worker).
-behaviour(gen_server).
-include("include/worker_net.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {pid :: pid(),
		job :: #wn_job{},
		workdir :: string(),
		olddir :: string(),
		commands :: [string()],
		port :: port()
	       }).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(string(),pid(),#wn_job{}) -> {ok,pid()} | term()).	     
start_link(NodeRoot,Pid,WnJob) ->
    gen_server:start_link(?MODULE, {NodeRoot,Pid,WnJob}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({NodeRoot,Pid,WnJob}) ->
    gen_server:cast(self(),start),
    {ok, #state{pid = Pid,
		job = WnJob,
		olddir = (fun({ok,Curr}) -> Curr end)(file:get_cwd()),
		workdir = wn_util:work_dir(NodeRoot,WnJob)}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(start, State) ->
    ok = filelib:ensure_dir(State#state.workdir),
    ok = file:set_cwd(State#state.workdir),
    WnJob = State#state.job,
    case get_work_files(WnJob) of
	{ok,_} ->
	    wn_job_keeper:info(State#state.pid,file_fetching_done),
	    wn_job_keeper:info(State#state.pid,executing_commands),
	    timer:send_after(WnJob#wn_job.timeout,self(),timeout),
	    [C|Commands] = (State#state.job)#wn_job.commands,
	    wn_job_keeper:info(State#state.pid,{executing,C}),
	    Port = erlang:open_port({spawn,C},[eof,{line,2048}]),
	    {noreply,State#state{commands = Commands,port = Port}};
	Err ->
	    ok = file:set_cwd(State#state.olddir),
	    ok = wn_util:clean_up(State#state.workdir),
	    wn_job_keeper:info(State#state.pid,{file_fetching_failed,Err}),
	    wn_job_keeper:info(State#state.pid,ending),
	    {stop,file_fetching_failed,State}
    end.

handle_info({Port,{data,{eol,D}}},#state{port = Port} = State) ->
    wn_job_keeper:progress(State#state.pid,D),
    {noreply,State};
handle_info({Port,eof},#state{port = Port} = State) ->
    case State#state.commands of
	[] ->
	    wn_job_keeper:info(State#state.pid,no_more_commands),
	    wn_job_keeper:info(State#state.pid,building_result_tgz),
	    ok = file:set_cwd(State#state.olddir),
            Keeper = State#state.pid,
            TimeMarker = wn_util:time_marker(),
            wn_job_keeper:done(Keeper,TimeMarker),
            Job = wn_job_keeper:get_job(Keeper),
            Logs = wn_job_keeper:logs(Keeper),
            {Id,Name} = make_tgz_result(TimeMarker,
                                        State#state.workdir,
                                        Job,Logs),
            ok = wn_file_layer:add_file(#wn_file{id = Id,
                                                 file = Name,
                                                 resides = node()}),
            file:delete(Name),
            wn_job_keeper:set_stored_result(Keeper,Id),
	    {stop,normal,State};
	[C|Commands] ->
	    wn_job_keeper:info(State#state.pid,{executing,C}),
	    NewPort = erlang:open_port({spawn,C},[eof,{line,2048}]),
	    {noreply,State#state{commands = Commands,port = NewPort}}
    end;    
handle_info(timeout, State) ->
    wn_job_keeper:info(State#state.pid,timeout_on_job),
    {stop,timeout,State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_work_files(WnJob) ->    
    lists:foldl(
      fun(_WnFile,{error,X}) -> {error,X};
	 (WnFile,{ok,_}) ->
	      wn_file_layer:retrieve_file(WnFile#wn_file.resides,
					  WnFile#wn_file.id)
      end,{ok,1},WnJob#wn_job.files).

make_tgz_result(TimeMarker,Dir,WnJob,Logs) ->
    Id = WnJob#wn_job.id++"_result",
    Name = Id++"_"++wn_util:time_marker_to_string(TimeMarker)++".tgz",
    {ok,TarFile} = erl_tar:open(Name,[write,compressed]),
    Files = filelib:wildcard(Dir++"*"),
    lists:foreach(
      fun(File) ->
              ok = erl_tar:add(TarFile,File,filename:basename(File),[])
      end,Files),
    LogStr = lists:foldl(
               fun({TimeMark,Entry},Str) ->
                       Str++io_lib:format("~p : ~p~n",[TimeMark,Entry])
               end,"",lists:sort(lists:append([Lines || {_,Lines} <- Logs]))),
    ok = erl_tar:add(TarFile,erlang:list_to_binary(LogStr),"Log.txt",[]),
    erl_tar:close(TarFile),
    {Id,Name}.
