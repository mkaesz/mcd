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
    self.setup_db(self)
    self.setup_models(self)
  end,

  setup_db = function(self)
      box.once('init', function()
          cnt_models = self.tablelength(self.models)
          log.info("Found " .. cnt_models .. " database schemas.")

          for i=1,cnt_models,1 do
            box.schema.create_space(self.models[i])
            box.space.product:create_index(
                "primary", {type = 'hash', parts = {1, 'unsigned'}}
            )
            box.space.product:create_index(
                "status", {type = "tree", parts = {2, 'str'}}
            )
          end
      end)
  end,

  setup_models = function(self)
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
          self.product_model = compiled_model
          log.info('Successfully started the application!')
          return true
        else
          log.error('Model' .. self.models[i] .. ' compilation failed')
        end
      else
        log.info('Model' .. self.models[i] .. ' creation failed')
      end
      return false
    end
  end,

  get_products = function(self)
    local result={}
    for _, tuple in box.space.product:select{} do
      local ok, product = self.product_model.unflatten(tuple)
      table.insert(result, product)
      return result
    end
  end,

  add_product = function(self, product)
    local ok, tuple = self.product_model.flatten(product)
    if not ok then
      return false
    end
    box.space.product:replace(tuple)
  end,

  tablelength = function(T)
    local count = 0
    for _ in pairs(T) do count = count + 1 end
    return count
  end
}

return msc
