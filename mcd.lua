local fiber = require('fiber')
local avro = require('avro_schema')
local log = require('log')
local json = require('json')
local errno = require('errno')
local fio = require('fio')

local msc = {
  product_model = {},
  models = {"product"},

  start = function(self)
    cnt_models = self.tablelength(self.models)
    log.info("Found " .. cnt_models .. " models.")

    for i=1,cnt_models,1 do
      local f = fio.open('models/' .. self.models[i] .. '.avsc', {'O_RDONLY' })
      if not f then
        error("Failed to open file: " .. self.models[i] .. ".avsc with Error: " .. errno.strerror())
      end
      local data = f:read(4096)
      f:close()

      local ok_m, model = avro.create(json.decode(data))

      if ok_m then
        log.info("Created model '" .. self.models[i] .. "'" )
        -- compile models
        local ok_cm, compiled_model = avro.compile(model)
        if ok_cm then
          log.info("Compiled model '" .. self.models[i] .. "'" )
          -- start the game
          -- self.product_model = compiled_model
          log.info('Successfully started the application!')
          return true
        else
          log.error('Schema compilation failed')
        end
      else
        log.info('Schema creation failed')
      end
      return false
    end
  end,

  tablelength = function(T)
    local count = 0
    for _ in pairs(T) do count = count + 1 end
    return count
  end
}

return msc
