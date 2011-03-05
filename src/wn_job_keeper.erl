%%%-------------------------------------------------------------------
%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2011, Gianfranco
%%% Created : 13 Jan 2011 by Gianfranco <zenon@zen.local>
%%%-------------------------------------------------------------------
-module(wn_job_keeper).
-behaviour(gen_fsm).
-include("include/worker_net.hrl").

%% API
-export([start_link/1,done/2,progress/2,info/2,stream/2,
	 signal/1,get_result/1,get_done/1,get_job/1,
	 cancel/1, get_stored_result/1,
         logs/1,set_stored_result/2,delete/1
        ]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
	 handle_info/3, terminate/3, code_change/4]).

-record(state, {job :: #wn_job{},
		info :: [{time(),term()}],
		result :: [{time(),term()}],
                stored_result :: string(),                
		stream :: [pid()],
		done_at :: undefined | time_marker()
	       }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(WnJob) ->
    gen_fsm:start_link(?MODULE, WnJob, []).

-spec(done(pid(),time_marker()) -> ok).
done(Pid,TimeMarker) ->
    gen_fsm:send_all_state_event(Pid,{done,TimeMarker}).

-spec(progress(pid(),term()) -> ok).
progress(Pid,X) ->
    gen_fsm:send_all_state_event(Pid,{progress,X}).

-spec(info(pid(),term()) -> ok).
info(Pid,X) ->
    gen_fsm:send_all_state_event(Pid,{info,X}).

-spec(stream(pid(),pid()) -> ok).
stream(Pid,StreamPid) ->
    gen_fsm:send_all_state_event(Pid,{stream,StreamPid}).

-spec(signal(pid()) -> {ok,#wn_job{}} | {error,taken}).
signal(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,signal).

-spec(get_result(pid()) -> [string()]).
get_result(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,get_result).

-spec(get_done(pid()) -> {ok,time_marker()} | {error,not_done}).	     
get_done(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,get_done).

-spec(get_job(pid()) -> #wn_job{}).
get_job(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,get_job).

-spec(cancel(pid()) -> ok | {error,term()}).
cancel(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,cancel).

-spec(logs(pid()) -> [{info|result,[{time(),term()}]}]).
logs(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,get_logs).

-spec(set_stored_result(pid(),string()) -> ok).
set_stored_result(Pid,Id) ->
    gen_fsm:sync_send_all_state_event(Pid,{stored_result,Id}).

-spec(get_stored_result(pid()) -> {ok,string()} | {error,term()}).             
get_stored_result(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,get_stored_result).

-spec(delete(pid()) -> ok | {error,term()}).
delete(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid,delete).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init(WnJob) ->
    {ok, waiting, #state{job = WnJob,
			 info = [],
			 result = [],
			 stream = []
			}}.

handle_event({done,TimeMarker}, working, #state{info = Info} = State) ->
    Now = now(),
    stream_msg(State#state.stream,{Now,done}),
    {next_state, done, State#state{info = [{Now,done}|Info],
				   done_at = TimeMarker
				  }};
handle_event({progress,X},working,#state{result = Result} = State) ->
    Now = now(),
    stream_msg(State#state.stream,{Now,X}),
    {next_state,working,State#state{result = [{Now,X}|Result]}};
handle_event({info,X},working,#state{info = Info} = State) ->
    Now = now(),
    stream_msg(State#state.stream,{Now,X}),
    {next_state,working,State#state{info = [{Now,X}|Info]}};
handle_event({stream,Pid},X,#state{stream = Stream} = State) ->
    replay(Pid,State),
    {next_state,X,State#state{stream=[Pid|Stream]}}.

handle_sync_event(delete,_,done,State) ->
    {stop,normal,ok,State};
handle_sync_event(delete,_,X,State) ->
    {reply,{error,not_done},X,State};
    
handle_sync_event({stored_result,Id},_,X,State) ->
    {reply,ok,X,State#state{stored_result = Id}};

handle_sync_event(get_logs,_From,X,State) ->
    {reply,[{info,State#state.info},
            {result,State#state.result}],X,State};

handle_sync_event(get_stored_result,_,X,State) ->
    {reply,
     case State#state.stored_result of
         undefined -> {error,no_result};
         R -> {ok,R}
     end,X,State};

handle_sync_event(cancel,_From,waiting,State) ->
    {stop,normal,ok,State};
handle_sync_event(cancel,_From,X,State) ->
    {reply,{error,X},X,State};
handle_sync_event(get_job,_From, X, State) ->
    {reply,State#state.job,X,State};
handle_sync_event(signal, _From, waiting, State) ->
    {reply,{ok,State#state.job}, working, State};
handle_sync_event(signal,_From,X,State) ->
    {reply,{error,taken},X,State};
handle_sync_event(get_result,_From,X,State) ->
    {reply,[ Z || {_,Z} <- State#state.result],X,State};
handle_sync_event(get_done,_From,done,State) ->
    {reply,{ok,State#state.done_at},done,State};
handle_sync_event(get_done,_From,X,State) ->
    {reply,{error,not_done},X,State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
replay(Pid,#state{info = Info,result=Result}) ->
    lists:foreach(fun(Entry) -> Pid ! Entry end,
		  lists:sort(Info++Result)).

stream_msg(List,X) ->
    lists:foreach(fun(Pid) -> Pid ! X end,List). 
