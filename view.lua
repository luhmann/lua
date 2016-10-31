--- This is a quite reckless but functioning second script to replace json-values in redis-keys
-- it is bad one edge cases but faster

--- Function to get a redis key
-- @name redisget
-- @tparam string the rediskey you want to get
-- @treturn string the value
local function redisget(key)
    return redis.call('GET', key)
end

--- Maps function `f(key, value)` on all key-value pairs. Collects
-- and returns the results as a table.
-- <br/><em>Aliased as `collect`</em>.
-- @name map
-- @tparam table t a table
-- @tparam function f  an iterator function, prototyped as `f(key, value, ...)`
-- @tparam[opt] vararg ... Optional extra-args to be passed to function `f`
-- @treturn table a table of results
local function map(t, f, ...)
    local _t = {}
    for index,value in pairs(t) do
        _t[index] = f(index,value,...)
    end
    return _t
end

local function merge(first, second)
    local hash = {}
    for k,v in pairs(second) do
        local key = tostring(first[k])
        redis.log(redis.LOG_DEBUG, key)
        redis.log(redis.LOG_DEBUG, v)
        hash[key] = v
    end

    return hash
end

local keyPattern = '"(mv%-internal%-object%:v1%:de%:de%:live%:[^"]*)"'

redis.log(redis.LOG_DEBUG, 'Trying to get key: ' .. tostring(KEYS[1]))
local base = redisget(KEYS[1])
redis.log(redis.LOG_DEBUG, 'String is' .. tostring(base))

local function hasNoReferenceKey(input)
    return string.match(input, keyPattern) == nil
end

local function replace(input)
    local concat = {}
    for key in string.gmatch(input, keyPattern) do
        table.insert(concat, key)
    end

    local values = redis.call('MGET', unpack(map(concat, function(index, value) return (string.gsub(value, "\\/", "%/")) end ) ))
    local hash = merge(concat, values)
    local output = string.gsub(input, keyPattern, hash)

    if hasNoReferenceKey(output) == false then
        return replace(output)
    else
        return output
    end
end

if base ~= false then
    if hasNoReferenceKey(base) == false then
        return replace(base)
    else
        return base
    end
else
    return nil
end
