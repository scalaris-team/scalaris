-module(cloud_beh).

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").
-type state() :: term().

-callback init() -> ok.
-callback get_number_of_vms() -> non_neg_integer().
-callback add_vms(Count::non_neg_integer()) -> state().
-callback remove_vms(Count::non_neg_integer()) -> state().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
	 {init, 0},
     {get_number_of_vms, 0},
     {add_vms, 1},
	 {remove_vms, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.