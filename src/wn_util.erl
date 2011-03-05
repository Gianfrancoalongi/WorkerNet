%%% @author Gianfranco <zenon@zen.home>
%%% @copyright (C) 2011, Gianfranco
%%% Created : 19 Jan 2011 by Gianfranco <zenon@zen.home>
-module(wn_util).
-include("include/worker_net.hrl").
-export([file_dir/2,
	 work_dir/2,
	 clean_up/1,
	 file_root/1,
         time_marker_to_string/1,
         time_marker/0
	]).

-spec(time_marker() -> time_marker()).
time_marker() ->
    {date(),time(),now()}.

-spec(time_marker_to_string(time_marker()) -> string()).
time_marker_to_string({Date,Time,Now}) ->
    F = fun(X) -> string:join([integer_to_list(Y)||Y<-tuple_to_list(X)],"_")
        end,
    F(Date)++"_"++F(Time)++"_"++F(Now).
    
-spec(clean_up(string()) -> ok | {error,term()}).	     
clean_up(Path) ->
    case filelib:is_dir(Path) of
	true ->
	    {ok,Files} = file:list_dir(Path),
	    lists:foreach(
	      fun(File) -> clean_up(Path++"/"++File)
	      end,Files),
	    file:del_dir(Path);
	false ->
	    ok = file:delete(Path)
    end.

-spec(work_dir(string(),#wn_job{}) -> string()).	     
work_dir(NodeRoot,WnJob) ->
    work_root(NodeRoot)++WnJob#wn_job.id++"/".

-spec(file_dir(string(),#wn_file{}) -> string()).	     
file_dir(NodeRoot,WnFile) ->
    file_root(NodeRoot)++WnFile#wn_file.id++"/".

-spec(file_root(string()) -> string()).	     
file_root(NodeRoot) ->
    NodeRoot++atom_to_list(node())++"/Files/".

-spec(work_root(string()) -> string()).	     
work_root(NodeRoot) ->
    NodeRoot++atom_to_list(node())++"/Jobs/".
