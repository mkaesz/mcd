package.path = "~/projects/lue/?.lua;" .. package.path

local mcd = require('mcd')
box.cfg{listen=3301}
mcd:start()

function add(request, product)
  return {
    result=mcd:add_product(product)
  }
end

function map(request)
    return {
        map=mcd:get_products()
    }
end
