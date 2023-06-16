local ioe = require 'ioe'
local cjson = require 'cjson.safe'
local mqtt_app = require 'app.base.mqtt'

local sub_topics = {
    "v1/gateway/rpc"
}

function split(str,reps)
    local resultStrList = {}
    string.gsub(str,'[^'..reps..']+',function ( w )
        table.insert(resultStrList,w)
    end)
    return resultStrList
end
function revtable(t)
	local newtable = {}
	for k,v in pairs(t) do
		newtable[v]=true
	end
	return newtable
end
function table_not_nil(t)
    if next(t) == nil then
        return false
    else
        return true
    end
end
--- 注册对象(请尽量使用唯一的标识字符串)
local app = mqtt_app:subclass("_ThingsBoard_APP")
--- 设定应用最小运行接口版本(目前版本为1,为了以后的接口兼容性)
app.static.API_VER = 4

---
-- 应用对象初始化函数
-- @param name: 应用本地安装名称。 如modbus_com_1
-- @param sys: 系统sys接口对象。参考API文档中的sys接口说明
-- @param conf: 应用配置参数。由安装配置中的json数据转换出来的数据对象
function app:initialize(name, sys, conf)
	--- 更新默认配置
	if conf.username ~= nil then
	    if #conf.username>0 then
        	conf.username = conf.username
    	else
    	    conf.username = "demo"
    	end
	end
	conf.server = conf.server or "dongbala.top"
	conf.port = conf.port or 1883
	
	if conf.period ~= nil then
    	conf.period = conf.period
	else
	    conf.period = 10
	end
	if conf.devs ~= nil then
    	if #conf.devs>0 then
    	    self._filters = revtable(split(conf.devs,','))
    	end
	end

	if conf.enable_batch ~= nil then
        self._enable_batch = conf.enable_batch
	end
-- 	conf.enable_tls = conf.enable_tls or false
-- 	conf.tls_cert = "root_cert.pem"
    
	--- 基础类初始化
	mqtt_app.initialize(self, name, sys, conf)
	
    self._log = sys:logger()
    self._mqtt_topic_prefix = sys:id()
    self._gate_sn = sys:id()
    self._devsinst = {}
--  self._log:info("_devsinst is::", cjson.encode(self._devsinst))
-- 	self._log:info("_filters is::", cjson.encode(self._filters))
end


-- 收到数据变化
function app:on_input(src_app, sn, input, prop, value, timestamp, quality)
    -- self._log:info("sn is::", sn)
    if self._filters~=nil then
        -- self._log:info("sn is::", sn)
        if (self._filters[sn]) then
            mqtt_app.on_input(self, src_app, sn, input, prop, value, timestamp, quality)
        end
    else
        mqtt_app.on_input(self, src_app, sn, input, prop, value, timestamp, quality)
    end
end

-- 上传数据
function app:on_publish_data(key, value, timestamp, quality)
    -- self._log:info("val is::", key, value, timestamp, quality)
	local sn, input, prop = string.match(key, '^([^/]+)/([^/]+)/(.+)$')
    if (self._devsinst[sn]) then
    	local msg = {}
    	msg[sn.."/"..self._devsinst[sn]] = {
    	    	{
                  ts =  timestamp*1000,
                  values =  { [input] = value}
            	}
    	}
    	-- 	self._log:info("val is::", cjson.encode(msg))
        ret =  self:publish("v1/gateway/telemetry", cjson.encode(msg), 0, false)
        -- 	self._log:info("vret::", ret)
        return true
    end

end

-- 批量上传数据
function app:on_publish_data_list(val_list)
    -- self._log:info("val_list is::", cjson.encode(val_list))
    if self._enable_batch ~= nil and self._enable_batch==false then
    	for _, v in ipairs(val_list) do
    		self:on_publish_data(table.unpack(v))
    	end
    	return true
    
    end
    
    local data = {}
    for _, v in ipairs(val_list) do
        local key = v[1]
        local value = v[2]
        local timestamp = v[3]
        local sn, input, prop = string.match(key, '^([^/]+)/([^/]+)/(.+)$')
        if (self._devsinst[sn]) then
            if(data[sn .. "/"..self._devsinst[sn]] == nil) then
                data[sn .. "/"..self._devsinst[sn]] = {}
            end
        	local msg = {
                      ts =  timestamp*1000,
                      values =  { [input] = value}
                	}
            data[sn .. "/"..self._devsinst[sn]][#data[sn .. "/"..self._devsinst[sn]] + 1] = msg
        end

    end

    -- self._log:info("batch_pubvals is::", cjson.encode(data))
    self:publish("v1/gateway/telemetry", cjson.encode(data), 0, false)
    return true
    
end

-- 上传缓存数据
function app:on_publish_cached_data_list(val_list)
    if self._enable_batch ~= nil and self._enable_batch==false then
    	for _, v in ipairs(val_list) do
    		self:on_publish_data(table.unpack(v))
    	end
    	return true
    end
    
    local data = {}
    local msg = {}
    for _, v in ipairs(val_list) do
        local key = v[1]
        local value = v[2]
        local timestamp = v[3]
        local sn, input, prop = string.match(key, '^([^/]+)/([^/]+)/(.+)$')
        if (self._devsinst[sn]) then
            if(data[sn .. "/"..self._devsinst[sn]] == nil) then
                data[sn .. "/"..self._devsinst[sn]] = {}
            end
        	local msg = {
                      ts =  timestamp*1000,
                      values =  { [input] = value}
                	}
            data[sn .. "/"..self._devsinst[sn]][#data[sn .. "/"..self._devsinst[sn]] + 1] = msg
        end

    end
    -- self._log:info("batch_pubvals is::", cjson.encode(data))
    self:publish("v1/gateway/telemetry", cjson.encode(data), 0, false)
	return true
end

-- 上传设备事件
function app:on_event(app, sn, level, data, timestamp)
    if self._filters~=nil then
        if (self._filters[sn]) then
        	local msg = {
        		app = app,
        		sn = sn,
        		level = level,
        		data = data,
        		timestamp = timestamp,
        	}
        	return self:publish(self._mqtt_topic_prefix.."/event", cjson.encode(msg), 1, false)
    	end
	end
end

-- 上传统计数据
function app:on_stat(app, sn, stat, prop, value, timestamp)
	local msg = {
		app = app,
		sn = sn,
		stat = stat,
		prop = prop,
		value = value,
		timestamp = timestamp,
	}
	return self:publish(self._mqtt_topic_prefix.."/statistics", cjson.encode(msg), 1, false)
end

-- 上传设备标签
function app:on_publish_devices(devices)
    -- self._log:info(" devices is:", cjson.encode(devices))
    local newdevs = {}
    if self._filters~=nil then
        -- self._log:info(" devices length:", #devices)
        for k, v in pairs(devices) do
            -- self._log:info(k.." is::", cjson.encode(self._filters[k]))
            if (self._filters[k]) then
                local tags = {}
                for i, val in pairs(v) do
                    -- self._log:info(i.." is::", cjson.encode(val))
                    if (i ~= "meta") then
                        for tagkey, tagval in pairs(val) do
                            local tagvt = tagval.vt
                            if(tagval.vt == nil)then
                                tagvt = "float"
                            end
                            tags[tagval.name] = {desc = tagval.desc, vt = tagvt}
                        end
                    else
                        -- self._log:info("_meta inst is::", val.inst)
                        self._devsinst[k] = val.inst
                    end
                end
                newdevs[k] = {[k.."/"..self._devsinst[k]] = tags}
            end
        end
    else
        for k, v in pairs(devices) do
            local tags = {}
            for i, val in pairs(v) do
                    if (i ~= "meta") then
                        for tagkey, tagval in pairs(val) do
                            tags[tagval.name] = {desc = tagval.desc, vt = tagval.vt}
                        end
                    else
                        self._devsinst[k] = val.inst
                    end
            end
            newdevs[k] = {[k.."/"..self._devsinst[k]] = tags}
        end
    end

    -- self._log:info("_devsinst is::", cjson.encode(self._devsinst))
    
    for pkey, qval in pairs(newdevs) do
        -- self._log:info("newdevs is:", cjson.encode(qval))
        local newdevice = {device = pkey.."/"..self._devsinst[pkey]}
	    self:publish("v1/gateway/connect", cjson.encode(newdevice), 1, true)
        self:publish("v1/gateway/attributes", cjson.encode(qval), 1, true)
    end
    return true

end

-- 收到平台消息
function app:on_mqtt_message(packet_id, topic, payload, qos, retained)
    self._log:info("subscribe message::",packet_id, topic, payload)
    

		local data = cjson.decode(payload)
		local traceid = data.data.id
		local dev_sn = split(data.device, "/")[1]
		local method = data.data.method
		self._log:info("payload params::", cjson.encode(data.data.params))
		if(method=="setOutput")then
    		local output = data.data.params.name
    		local value = data.data.params.value
    		self._log:info("payload parse::",traceid, method, dev_sn, output, value)
    		local device, err = self._api:get_device(dev_sn)
    		if not device then
    			self._log:error('Cannot parse payload!')
    			return self:publish("v1/gateway/rpc", cjson.encode({device=data.device ,id=traceid, data={success=false}}), 1, true)
    		end
    		local priv = { id = traceid, dev_sn = dev_sn, input = output }
    		local r, err = device:set_output_prop(output, 'value', value, ioe.time(), priv)
    		if not r then
    			self._log:error('Set output prop failed!', err)
    			return self:publish("v1/gateway/rpc", cjson.encode({device=data.device ,id=traceid, data={success=false}}), 1, true)
    		end
            self._log:info(output .. " "..method.." "..value.." success")
    		return self:publish("v1/gateway/rpc", cjson.encode({device=data.device ,id=traceid, data={success=true}}), 1, true)
    	end
    	if(method=="getOutput")then
    	    local output = data.data.params.name
    		self._log:info("payload parse::",traceid, method, dev_sn, output)
    		local device, err = self._api:get_device(dev_sn)
    		local r, err = device:get_output_prop(output, 'value')
    		if r then
    	        return self:publish("v1/gateway/rpc", cjson.encode({device=data.device ,id=traceid, data={success=true}}), 1, true)
    	    end
        end
    	return self:publish("v1/gateway/rpc", cjson.encode({device=data.device ,id=traceid, data={success=false}}), 1, true)

	--print(...)
end

-- 返回处理结果
function app:on_output_result(app_src, priv, result, err)
	if result then
		return self:publish(self._mqtt_topic_prefix.."/result/output", cjson.encode({id=priv.id, result=true}), 1, true)
	else
		return self:publish(self._mqtt_topic_prefix.."/result/output", cjson.encode({id=priv.id, result=false}), 1, true)
	end
end

-- 连接平台
function app:on_mqtt_connect_ok()
	for _, v in ipairs(sub_topics) do
		self:subscribe(v, 1)
	end
	local freeioe = {device = self._gate_sn.."/ThingsLink"}
	self:publish("v1/gateway/connect", cjson.encode(freeioe), 1, true)
	return self:publish(self._mqtt_topic_prefix.."/status", cjson.encode({device=self._mqtt_topic_prefix, status="ONLINE"}), 1, true)
end

-- 连接断开
function app:mqtt_will()
    local freeioe = {device = self._gate_sn.."/ThingsLink"}
	self:publish("v1/gateway/disconnect", cjson.encode(freeioe), 1, true)
-- 	return self._mqtt_topic_prefix.."/status", cjson.encode({device=self._mqtt_topic_prefix, status="OFFLINE"}), 1, true
end

--- 返回应用对象
return app
