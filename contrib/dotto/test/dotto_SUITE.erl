-module(dotto_SUITE).
-compile(export_all).

all() ->
    [add_to_empty_list, add_to_empty_map, add_start_list, add_middle_list,
     override_map_value, add_level_1_map, add_level_1_existing_map,
     add_level_2_list, add_level_2_not_list, add_level_2_not_map,

     replace_to_empty_list, replace_to_empty_map, replace_start_list,
     replace_middle_list, replace_end_list, replace_map_value,
     replace_level_1_map, replace_level_1_inexistent_map, replace_level_2_list,
     replace_level_2_not_list, replace_level_2_not_map,
    
     fetch_default,

     remove_empty_list, remove_item_list, remove_item_map].

add_to_empty_list(_) ->
    {ok, [42]} = dotto:add([], [<<"-">>], 42).

add_to_empty_map(_) ->
    {ok, #{foo := 42}} = dotto:add(#{}, [foo], 42).

add_start_list(_) ->
    {ok, [42, 1]} = dotto:add([1], [0], 42).

add_middle_list(_) ->
    {ok, [1, 42, 2]} = dotto:add([1, 2], [1], 42).

override_map_value(_) ->
    {ok, #{foo := 42}} = dotto:add(#{foo => 1}, [foo], 42).

add_level_1_map(_) ->
    {ok, #{foo := #{bar := 42}}} = dotto:add(#{foo => #{}}, [foo, bar], 42).

add_level_1_existing_map(_) ->
    {ok, #{foo := #{bar := 42}}} = dotto:add(#{foo => #{bar => 12}}, [foo, bar], 42).

add_level_2_list(_) ->
    {ok, #{foo := #{bar := [1, 42, 2]}}} = dotto:add(#{foo => #{bar => [1, 2]}}, [foo, bar, 1], 42).

add_level_2_not_list(_) ->
    {error, {cantset, notalist, 1, 42}} = dotto:add(#{foo => #{bar => notalist}}, [foo, bar, 1], 42).

add_level_2_not_map(_) ->
    {error, {cantset, notamap, baz, 42}} = dotto:add(#{foo => #{bar => notamap}}, [foo, bar, baz], 42).

%% replace


replace_to_empty_list(_) ->
    {error, {invalidindex, [], <<"-">>}} = dotto:replace([], [<<"-">>], 42).

replace_to_empty_map(_) ->
    {error, {notfound, #{}, foo}} = dotto:replace(#{}, [foo], 42).

replace_start_list(_) ->
    {ok, [42]} = dotto:replace([1], [0], 42),
    {ok, [42, 2]} = dotto:replace([1, 2], [0], 42).

replace_middle_list(_) ->
    {ok, [1, 42, 3]} = dotto:replace([1, 2, 3], [1], 42).

replace_end_list(_) ->
    {ok, [42]} = dotto:replace([1], [0], 42),
    {ok, [1, 42]} = dotto:replace([1, 2], [1], 42),
    {ok, [1, 2, 42]} = dotto:replace([1, 2, 3], [2], 42).

replace_map_value(_) ->
    {ok, #{foo := 42}} = dotto:replace(#{foo => 1}, [foo], 42).

replace_level_1_map(_) ->
    {ok, #{foo := #{bar := 42}}} = dotto:replace(#{foo => #{bar => 1}}, [foo, bar], 42).

replace_level_1_inexistent_map(_) ->
    {error, {notfound, #{}, bar}} = dotto:replace(#{foo => #{}}, [foo, bar], 42).

replace_level_2_list(_) ->
    {ok, #{foo := #{bar := [1, 42, 3]}}} = dotto:replace(#{foo => #{bar => [1, 2, 3]}}, [foo, bar, 1], 42).

replace_level_2_not_list(_) ->
    {error, {notfound, notalist, 1}} = dotto:replace(#{foo => #{bar => notalist}}, [foo, bar, 1], 42).

replace_level_2_not_map(_) ->
    {error, {notfound, notamap, baz}} = dotto:replace(#{foo => #{bar => notamap}}, [foo, bar, baz], 42).

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

remove_item_map(_) ->
    {ok, #{}} = dotto:remove(#{a => 1}, [a]),
    {ok, #{b := 2}} = dotto:remove(#{a => 1, b => 2}, [a]),
    {ok, #{a := #{c := 3}}} = dotto:remove(#{a => #{b => 1, c => 3}}, [a, b]),
    {error, {notfound, #{}, a}} = dotto:remove(#{}, [a]),
    {error, {notfound, #{}, a}} = dotto:remove(#{}, [a, b]),
    {error, {notfound, #{c := 2}, b}} = dotto:remove(#{a => #{c => 2}}, [a, b]).

% fetch

fetch_default(_) ->
    Obj1 = #{a => 1},
    SubObj1 = #{c => 2},
    Obj2 = #{a => 1, b => SubObj1},

    {ok, 1} = dotto:fetch(Obj1, [a]),
    {error, {notfound, Obj1, b}} = dotto:fetch(Obj1, [b]),
    {ok, 2} = dotto:fetch(Obj1, [b], 2),

    {ok, 2} = dotto:fetch(Obj2, [b, c]),
    {error, {notfound, SubObj1, d}} = dotto:fetch(Obj2, [b, d]),
    {ok, 3} = dotto:fetch(Obj2, [b, d], 3).
