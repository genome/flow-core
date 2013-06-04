local dest_hash_key = KEYS[1]

for i = 2, #KEYS do
    local src_hash_key = KEYS[i]

    for j, hkey in pairs(redis.call("HKEYS", src_hash_key)) do
        local rv = redis.call("HSETNX", dest_hash_key, hkey,
                redis.call("HGET", src_hash_key, hkey))

        if rv == 0 then
            return {-1, string.format("Conflicting data in key (%s)", hkey)}
        end
    end
end

return {0, "Success"}
