-module(dotto_SUITE).
-compile(export_all).

-define(EMPTY, dict:new()).
-define(DICT(X), dict:from_list(X)).
-define(DICT(X,Y), ?DICT([{X,Y}])).

all() ->
    [add_to_empty_list, add_to_empty_dict, add_start_list, add_middle_list,
     override_dict_value, add_level_1_dict, add_level_1_existing_dict,
     add_level_2_list, add_level_2_not_list, add_level_2_not_dict,

     replace_to_empty_list, replace_to_empty_dict, replace_start_list,
     replace_middle_list, replace_end_list, replace_dict_value,
     replace_level_1_dict, replace_level_1_inexistent_dict, replace_level_2_list,
     replace_level_2_not_list, replace_level_2_not_dict,
    
     fetch_default,

     remove_empty_list, remove_item_list, remove_item_dict].

add_to_empty_list(_) ->
    {ok, [42]} = dotto:add([], [<<"-">>], 42).

add_to_empty_dict(_) ->
    Expected = ?DICT(foo, 42),
    {ok, Expected} = dotto:add(?EMPTY, [foo], 42).

add_start_list(_) ->
    {ok, [42, 1]} = dotto:add([1], [0], 42).

add_middle_list(_) ->
    {ok, [1, 42, 2]} = dotto:add([1, 2], [1], 42).

override_dict_value(_) ->
    Start = ?DICT(foo, 1),
    Expected = ?DICT(foo, 42),
    {ok, Expected} = dotto:add(Start, [foo], 42).

add_level_1_dict(_) ->
    Start = ?DICT(foo, ?EMPTY),
    Expected = ?DICT(foo, ?DICT(bar, 42)),
    {ok, Expected} = dotto:add(Start, [foo, bar], 42).

add_level_1_existing_dict(_) ->
    Start = ?DICT(foo, ?DICT(bar, 12)),
    Expected = ?DICT(foo, ?DICT(bar, 42)),
    {ok, Expected} = dotto:add(Start, [foo, bar], 42).

add_level_2_list(_) ->
    Start = ?DICT(foo, ?DICT(bar, [1,2])),
    Expected = ?DICT(foo, ?DICT(bar, [1,42,2])),
    {ok, Expected} = dotto:add(Start, [foo, bar, 1], 42).

add_level_2_not_list(_) ->
    Start = ?DICT(foo, ?DICT(bar, notalist)),
    {error, {cantset, notalist, 1, 42}} = dotto:add(Start, [foo, bar, 1], 42).

add_level_2_not_dict(_) ->
    Start = ?DICT(foo, ?DICT(bar, notadict)),
    {error, {cantset, notadict, baz, 42}}=dotto:add(Start, [foo, bar, baz], 42).

%% replace


replace_to_empty_list(_) ->
    {error, {invalidindex, [], <<"-">>}} = dotto:replace([], [<<"-">>], 42).

replace_to_empty_dict(_) ->
    Empty = ?EMPTY,
    {error, {notfound, Empty, foo}} = dotto:replace(Empty, [foo], 42).

replace_start_list(_) ->
    {ok, [42]} = dotto:replace([1], [0], 42),
    {ok, [42, 2]} = dotto:replace([1, 2], [0], 42).

replace_middle_list(_) ->
    {ok, [1, 42, 3]} = dotto:replace([1, 2, 3], [1], 42).

replace_end_list(_) ->
    {ok, [42]} = dotto:replace([1], [0], 42),
    {ok, [1, 42]} = dotto:replace([1, 2], [1], 42),
    {ok, [1, 2, 42]} = dotto:replace([1, 2, 3], [2], 42).

replace_dict_value(_) ->
    Start = ?DICT(foo, 1),
    Expected = ?DICT(foo, 42),
    {ok, Expected} = dotto:replace(Start, [foo], 42).

replace_level_1_dict(_) ->
    Start = ?DICT(foo, ?DICT(bar, 1)),
    Expected = ?DICT(foo, ?DICT(bar, 42)),
    {ok, Expected} = dotto:replace(Start, [foo, bar], 42).

replace_level_1_inexistent_dict(_) ->
    Start = ?DICT(foo, ?EMPTY),
    Nested = ?EMPTY,
    {error, {notfound, Nested, bar}} = dotto:replace(Start, [foo, bar], 42).

replace_level_2_list(_) ->
    Start = ?DICT(foo, ?DICT(bar, [1,2,3])),
    Expected = ?DICT(foo, ?DICT(bar, [1,42,3])),
    {ok, Expected} = dotto:replace(Start, [foo, bar, 1], 42).

replace_level_2_not_list(_) ->
    Start = ?DICT(foo, ?DICT(bar, notalist)),
    {error, {notfound, notalist, 1}} = dotto:replace(Start, [foo, bar, 1], 42).

replace_level_2_not_dict(_) ->
    Start = ?DICT(foo, ?DICT(bar, notadict)),
    {error, {notfound, notadict, baz}} = dotto:replace(Start, [foo, bar, baz], 42).

%% remove

remove_empty_list(_) ->
    {error, {notfound, [], 1}} = dotto:remove([], [1]),
    {error, {notfound, [], 0}} = dotto:remove([], [0]).

remove_item_list(_) ->
    {ok, []} = dotto:remove([42], [0]),
    {ok, [1]} = dotto:remove([1, 42], [1]),
    {ok, [1, 2]} = dotto:remove([1, 42, 2], [1]),
    {ok, [42, 2]} = dotto:remove([1, 42, 2], [0]),
    {ok, [1, [2, 3]]} = dotto:remove([1, [2, 42, 3]], [1, 1]),
    {error, {notfound, [], 0}} = dotto:remove([], [0]),
    {error, {notfound, [10, 11], 2}} = dotto:remove([10, 11], [2]).

remove_item_dict(_) ->
    A = ?DICT([{a,1},{b,2}]),
    B = ?DICT(a, 1),
    C = ?EMPTY,
    {ok, B} = dotto:remove(A, [b]),
    {ok, C} = dotto:remove(B, [a]),

    Sub = ?DICT(c, 2), 
    Start = ?DICT(a, ?DICT([{b, 1}, {c, 2}])),
    Expected = ?DICT(a, Sub),
    {ok, Expected} = dotto:remove(Start, [a, b]),
    {error, {notfound, C, a}} = dotto:remove(C, [a]),
    {error, {notfound, C, a}} = dotto:remove(C, [a, b]),
    {error, {notfound, Sub, b}} = dotto:remove(Expected, [a, b]).

% fetch

fetch_default(_) ->
    Obj1 = ?DICT(a, 1),
    SubObj1 = ?DICT(c, 2),
    Obj2 = ?DICT([{a, 1}, {b, SubObj1}]),

    {ok, 1} = dotto:fetch(Obj1, [a]),
    {error, {notfound, Obj1, b}} = dotto:fetch(Obj1, [b]),
    {ok, 2} = dotto:fetch(Obj1, [b], 2),

    {ok, 2} = dotto:fetch(Obj2, [b, c]),
    {error, {notfound, SubObj1, d}} = dotto:fetch(Obj2, [b, d]),
    {ok, 3} = dotto:fetch(Obj2, [b, d], 3).

