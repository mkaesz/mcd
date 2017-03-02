local fiber = require('fiber')
local avro = require('avro_schema')
local log = require('log')
local json = require('json')
local errno = require('errno')
local fio = require('fio')

local models = {product}

for i=1,#models,1 do
local f = fio.open('models/player.avsc', {'O_RDONLY' })
if not f then
    error("Failed to open file: "..errno.strerror())
end
local pla_data = f:read(4096)

local f2 = fio.open('/opt/tarantool/models/pokemon.avsc', {'O_RDONLY' })
if not f2 then
    error("Failed to open file: "..errno.strerror())
end
local pok_data = f2:read(4096)


local game = {
    wgs84 = 4326, -- WGS84 World-wide Projection (Lon/Lat)
    nationalmap = 2163, -- US National Atlas Equal Area projection (meters)
    catch_distance = 100,
    respawn_time = 60,
    state = {
        ACTIVE='active',
        CAUGHT='caught'
    },
    player_model = {},
    pokemon_model = {},

    ID = 1,
    STATUS = 2,

    -- create game object
    start = function(self)
	box.once('init', function()
            box.schema.create_space('pokemons')
            box.space.pokemons:create_index(
                "primary", {type = 'hash', parts = {1, 'unsigned'}}
            )
            box.space.pokemons:create_index(
                "status", {type = "tree", parts = {2, 'str'}}
            )
        end)

        -- create models
        local ok_m, pokemon = avro.create(json.decode(pok_data))
        -- create spaces and indexes
        local ok_p, player = avro.create(json.decode(pla_data))
        if ok_p and ok_m then
            -- compile models
            local ok_cm, compiled_pokemon = avro.compile(pokemon)           
            local ok_cp, compiled_player = avro.compile(player)
            if ok_cp and ok_cm then
                -- start the game
                self.pokemon_model = compiled_pokemon
                self.player_model = compiled_player
                log.info('Started')
                return true
            else
                log.error('Schema compilation failed')
            end
        else
            log.info('Schema creation failed')
        end
        return false
    end,

    -- return pokemons list in map
    map = function(self)
        local result = {}
        for _, tuple in box.space.pokemons.index.status:pairs(
                self.state.ACTIVE) do
            local ok, pokemon = self.pokemon_model.unflatten(tuple)
            table.insert(result, pokemon)
        end
        return result
    end,

    -- add pokemon to map and store it in Tarantool
    add_pokemon = function(self, pokemon)
        pokemon.status = self.state.ACTIVE
        local ok, tuple = self.pokemon_model.flatten(pokemon)
        if not ok then
            return false
        end
        box.space.pokemons:replace(tuple)
        return true
    end,

    -- catch pokemon in location
    catch = function(self, pokemon_id, player)
        -- check player data
        local ok, tuple = self.player_model.flatten(player)
        if not ok then
            return false
        end
        -- get pokemon data
        local p_tuple = box.space.pokemons:get(pokemon_id)
        if p_tuple == nil then
            return false
        end
        local ok, pokemon = self.pokemon_model.unflatten(p_tuple)
        if not ok then
            return false
        end
        if pokemon.status ~= self.state.ACTIVE then
            return false
        end
        local m_pos = gis.Point(
            {pokemon.location.x, pokemon.location.y}, self.wgs84
        ):transform(self.nationalmap)
        local p_pos = gis.Point(
            {player.location.x, player.location.y}, self.wgs84
        ):transform(self.nationalmap)

        -- check catch distance condition
        if p_pos:distance(m_pos) > self.catch_distance then
            return false
        end
        -- try to catch pokemon
        local caught = math.random(100) >= 100 - pokemon.chance
        if caught then
            -- update and notify on success
            box.space.pokemons:update(
                pokemon_id, {{'=', self.STATUS, self.state.CAUGHT}}
            )
            self:notify(player, pokemon)
        end
        return caught
    end
}

return game
