package.path = "~/projects/lue/?.lua;" .. package.path

local mcd = require('mcd')
box.cfg{listen=3301}
mcd:start()
