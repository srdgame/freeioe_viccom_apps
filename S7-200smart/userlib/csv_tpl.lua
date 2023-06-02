local ftcsv = require 'ftcsv'
local log = require 'utils.log'

local tpl_dir = 'tpl/'

local function revtab(tab)
	local revtab = {}
	for k, v in pairs(tab) do
		revtab[v] = true
	end
	return revtab
end

local rwtype = {'RW','RO'}
local rev_rwtype = revtab(rwtype)
local dttype = {'bool', 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64', 'float', 'double','string'}
local rev_dttype = revtab(dttype)
local fctype = {'I', 'Q', 'M', 'DB', 'CT', 'TM'}
local rev_fctype = revtab(fctype)
local cloudtype = {'int', 'float', 'string'}
local rev_cloudtype = revtab(cloudtype)

local function load_tpl(name)
	local path = tpl_dir..name..'.csv'
	local t = ftcsv.parse(path, ",", {headers=false})

	local meta = {}
	local inputs = {}
	local outputs = {}
    log.debug("校验点表")
	for k,v in ipairs(t) do
		if #v > 1 then
			if v[1] == 'META' then
				meta.name = v[2]
				meta.desc = v[3]
				meta.series = v[4]
			end
			if v[1] == 'INPUT' then
			    if #v[2] > 0 then
    				local input = {
    					name = v[2],
    					desc = v[3],
    				}
    				-- if string.len(v[4]) > 0 then
    				-- 	input.vt = v[4]
    				-- end
    				input.unit = v[4]
    				
    				local U_rw = string.upper(v[5])
    				if rev_rwtype[U_rw] then
    				    input.rw = U_rw
    				else
    				    log.debug(v[2], "参数rw不合法",v[5])
    				    input.rw = 'RO'
    				end
    				local L_dt = string.lower(v[6])
    				if rev_dttype[L_dt] then
    				    input.dt = v[6]
        				local U_fc = string.upper(v[7])
        				if rev_fctype[U_fc] then
        				    input.fc = U_fc
            				if tonumber(v[8]) ~= nil then
                				input.dbnum = tonumber(v[8])
                				if tonumber(v[9]) ~= nil then
                    				input.saddr = tonumber(v[9])
                        				input.pos = tonumber(v[10]) or 0
                        				if tonumber(v[11]) ~= nil then
                            				input.rate = tonumber(v[11])
                            				local L_cdt = string.lower(v[12])
                            				if rev_cloudtype[L_cdt] then
                                				input.vt = L_cdt
                                			else
                                			    input.vt = 'string'
                                			end
                                			inputs[#inputs + 1] = input
                                		else
                                		    log.debug(v[2], "参数rate不能为空",v[11])
                        				end
                			    else
                			        log.debug(v[2], "参数saddr不能为空",v[9])
                				end
            			    else
            			        log.debug(v[2], "参数dbnum不能为空",v[8])
            				end
        				else
        				    log.debug(v[2], "参数fc不合法",v[7])
        				end
			        else
			            log.debug(v[2], "参数dt不合法",v[6])
    			    end
                else
                    log.debug("参数name不能为空")
				end
			end
			if v[1] == 'OUTPUT' then
				local output = {
					name = v[2],
					desc = v[3],
				}
				-- if string.len(v[4]) > 0 then
				-- 	input.vt = v[4]
				-- end
				input.unit = v[4]
				input.rw = v[5]
				input.dt = v[6]
				input.fc = v[7]
				input.dbnum = tonumber(v[8])
				input.saddr = tonumber(v[9])
				input.pos = tonumber(v[10])
				input.rate = tonumber(v[11])
				input.vt = v[12]
				-- log.info("@@@@@@",v[8])
				outputs[#outputs + 1] = output
			end
		end
	end

	return {
		meta = meta,
		inputs = inputs,
		outputs = outputs
	}
end

--[[
--local cjson = require 'cjson.safe'
local tpl = load_tpl('bms')
print(cjson.encode(tpl))
]]--

return {
	load_tpl = load_tpl,
	init = function(dir)
		tpl_dir = dir.."/tpl/"
	end
}
