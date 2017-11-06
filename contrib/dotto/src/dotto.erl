-module(dotto).
-export([apply/2, add/3, remove/2, replace/3, move/3, copy/3,
         test/3,
         fetch/2, fetch/3]).

apply(Ops, Obj) when is_list(Ops) ->
    do_apply(Ops, Obj, []);

apply({add, Path, Val}, Obj) -> add(Obj, Path, Val);
apply({remove, Path}, Obj) -> remove(Obj, Path);
apply({replace, Path, Val}, Obj) -> replace(Obj, Path, Val);
apply({move, FromPath, ToPath}, Obj) -> move(Obj, FromPath, ToPath);
apply({copy, FromPath, ToPath}, Obj) -> copy(Obj, FromPath, ToPath);
apply({test, Path, Val}, Obj) -> test(Obj, Path, Val).

do_apply([], Obj, []) ->
    {ok, Obj};
do_apply([], Obj, Errors) ->
    {error, Obj, Errors};

do_apply([{test, _Path, _Val}=Op|Ops], Obj, Errors) ->
    case dotto:apply(Op, Obj) of
        {ok, true} -> do_apply(Ops, Obj, Errors);
        {ok, false} -> do_apply(Ops, Obj, [{error, {testfail, Op}}|Errors]);
        Other -> do_apply(Ops, Obj, [Other|Errors])
    end;

do_apply([Op|Ops], Obj, Errors) ->
    case dotto:apply(Op, Obj) of
        {ok, NewObj} -> do_apply(Ops, NewObj, Errors);
        {error, Error} -> do_apply(Ops, Obj, [Error|Errors])
    end.

% this will only match if you try to replace the whole document with an empty path
add(_Obj, [], Val) ->
    {ok, Val};
add(Obj, [Field], Val) ->
    add_(Obj, Field, Val);

add(Obj, [Field|Fields], Val) ->
    case get_(Obj, Field) of
        {ok, FieldObj} ->
            case add(FieldObj, Fields, Val) of
                % XXX what happens if a "-" is in the middle of the path?
                {ok, NewVal} -> set_(Obj, Field, NewVal);
                Other -> Other
            end;
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end.

remove(Obj, [Field]) ->
    case get_(Obj, Field) of
        {ok, _FieldObj} ->
            del_(Obj, Field);
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end;

remove(Obj, [Field|Fields]) ->
    case get_(Obj, Field) of
        {ok, FieldObj} ->
            case remove(FieldObj, Fields) of
                {ok, NewVal} -> set_(Obj, Field, NewVal);
                Other -> Other
            end;
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end.

% this will only match if you try to replace the whole document with an empty path
replace(_Obj, [], Val) ->
    {ok, Val};
replace(Obj, [Field], Val) ->
    case get_(Obj, Field) of
        {ok, _FieldObj} ->
            set_(Obj, Field, Val);
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end;

replace(Obj, [Field|Fields], Val) ->
    case get_(Obj, Field) of
        {ok, FieldObj} ->
            case replace(FieldObj, Fields, Val) of
                {ok, NewVal} -> set_(Obj, Field, NewVal);
                Other -> Other
            end;
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end.

% XXX The "from" location MUST NOT be a proper prefix of the "path"
% location; i.e., a location cannot be moved into one of its children.
move(Obj, FromPath, ToPath) ->
    case fetch(Obj, FromPath) of
        {ok, Value} ->
            case remove(Obj, FromPath) of
                {ok, Obj1} ->
                    add(Obj1, ToPath, Value);
                Error -> Error
            end;
        Error -> Error
    end.

copy(Obj, FromPath, ToPath) ->
    case fetch(Obj, FromPath) of
        {ok, Value} ->
            add(Obj, ToPath, Value);
        Error -> Error
    end.

test(Obj, Path, Val) ->
    case fetch(Obj, Path) of
        {ok, Value} ->
            {ok, Value =:= Val};
        Other -> Other
    end.

% non RFC 6902 functions

fetch(Obj, []) ->
    {ok, Obj};

fetch(Obj, [Field|Fields]) ->
    case get_(Obj, Field) of
        {ok, FieldObj} ->
            fetch(FieldObj, Fields);
        notfound -> {error, {notfound, Obj, Field}};
        Other -> Other
    end.

fetch(Obj, Path, Default) ->
    case fetch(Obj, Path) of
        {ok, _} = Result -> Result;
        {error, _} -> {ok, Default}
    end.

% private api


% TODO: make this an option
add_(Obj, Field, Value) when is_map(Obj) andalso is_integer(Field) ->
    {ok, maps:put(integer_to_binary(Field), Value, Obj)};

add_(Obj, Field, Value) when is_map(Obj) ->
    {ok, maps:put(Field, Value, Obj)};

add_(Obj, <<"-">>, Value) when is_list(Obj) ->
    {ok, Obj ++ [Value]};

add_(Obj, Field, Value) when is_list(Obj) andalso is_integer(Field) ->
    {L1, L2} = lists:split(Field, Obj),
    {ok, L1 ++ [Value|L2]};

add_(Obj, Field, Value) ->
    {error, {cantset, Obj, Field, Value}}.

set_(Obj, Field, Value) when is_map(Obj) ->
    {ok, maps:put(Field, Value, Obj)};

set_(Obj, Field, Value) when is_list(Obj) andalso is_integer(Field) ->
    Result = case lists:split(Field, Obj) of
                 {[], [_|T]} -> [Value] ++ T;
                 {H1, [_|T]} -> H1 ++ [Value] ++ T;
                 % TODO: should droplast(H1)? I think not since it's 0 based
                 {H1, []} -> H1 ++ [Value]
             end,
    {ok, Result};


set_(Obj, Field, Value) ->
    {error, {cantset, Obj, Field, Value}}.

del_(Obj, Field) when is_map(Obj) ->
    {ok, maps:remove(Field, Obj)};

% XXX not sure if this case is in RFC 6902
del_(Obj, <<"-">>) when is_list(Obj) ->
    {ok, lists:droplast(Obj)};

del_(Obj, Field) when is_list(Obj) andalso is_integer(Field) ->
    Result = case lists:split(Field, Obj) of
                 {[], [_|T]} -> T;
                 {H1, [_|T]} -> H1 ++ T;
                 {H1, []} -> lists:droplast(H1)
             end,
    {ok, Result};

del_(Obj, Field) ->
    {error, {cantremove, Obj, Field}}.

get_(Obj, Field) when is_map(Obj) ->
    case maps:find(Field, Obj) of
        {ok, Value} -> {ok, Value};
        error -> notfound
    end;

get_(Obj, <<"-">>=Index) when is_list(Obj) ->
    {error, {invalidindex, Obj, Index}};

get_(Obj, Field) when is_list(Obj) andalso is_integer(Field) ->
    InsideList = Field >= 0 andalso Field  < length(Obj),
    if InsideList -> {ok, lists:nth(Field + 1, Obj)};
       true -> notfound
    end;

get_(_Obj, _Field) ->
    notfound.
