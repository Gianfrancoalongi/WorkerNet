%%% @author Gianfranco <zenon@zen.local>
%%% @copyright (C) 2010, Gianfranco
%%% Created : 10 Dec 2010 by Gianfranco <zenon@zen.local>

-type resource_spec() :: [{atom(),infinity| non_neg_integer()}].

-record(wn_resource,
	{name :: string(),
	 type :: resource_spec(),
	 resides :: node(),
	 pid :: pid() | undefined
	}).

-record(wn_file,
	{id :: string(),
	 file :: string(),
	 resides :: node()
	}).

-record(wn_job,
	{id :: string(),
	 files :: [#wn_file{}],
	 resources :: [atom()],
	 commands :: [string()],
	 timeout :: integer()
	 }).

-type date() :: {integer(),integer(),integer()}.
-type time() :: {integer(),integer(),integer()}.
-type now() :: {integer(),integer(),integer()}.
-type time_marker() :: {date(),time(),now()}.
