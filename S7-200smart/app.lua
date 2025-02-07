local class = require 'middleclass'
--- 导入需要的模块
local event = require 'app.event'
local sum = require 'hashings.sum'
local s7 = require 'snap7'
local basexx = require 'basexx'
local csv_tpl = require 'userlib.csv_tpl'
local conf_helper = require 'app.conf_helper'
local cjson = require 'cjson'

--- 注册对象(请尽量使用唯一的标识字符串)
local app = class("siemens_S7_200Smart")
--- 设定应用最小运行接口版本(目前版本为1,为了以后的接口兼容性)
app.API_VER = 2

local _dt_len_map = {
    bool = 1,
    int8 = 1,
    uint8 = 1,
    int16 = 1,
    uint16 = 1,
    int32 = 2,
    uint32 = 2,
    int64 = 4,
    uint64 = 4,
    float = 2,
    double = 4
}

local _byte_len_map = {
    bool = 1,
    int8 = 1,
    uint8 = 1,
    int16 = 2,
    uint16 = 2,
    int32 = 4,
    uint32 = 4,
    int64 = 8,
    uint64 = 8,
    float = 4,
    double = 8
}

local _dt_format = {
    bool = '>i1',
    int8 = '>i1',
    uint8 = '>I1',
    int16 = '>i2',
    uint16 = '>I2',
    int32 = '>i4',
    uint32 = '>I4',
    int64 = '>i8',
    uint64 = '>I8',
    float = '>f',
    double = '>D'
}

---
-- 应用对象初始化函数
-- @param name: 应用本地安装名称。 如modbus_com_1
-- @param sys: 系统sys接口对象。参考API文档中的sys接口说明
-- @param conf: 应用配置参数。由安装配置中的json数据转换出来的数据对象
function app:initialize(name, sys, conf)
    self._name = name
    self._sys = sys
    self._conf = conf
    --- 获取数据接口
    self._api = self._sys:data_api()
    --- 获取日志接口
    self._log = sys:logger()
    --- 设备实例
    self._devsxx = {}
    self._devsyy = {}

    self._log:debug("General_snap7_200Smart Application initlized")
end

--- 应用启动函数
function app:start()
    self._api:set_handler({
        --[[		--- 处理设备输入项数值变更消息，当需要监控其他设备时才需要此接口，并在set_handler函数传入监控标识
		on_input = function(app, sn, input, prop, value, timestamp, quality)
		end,
		]]
        on_output = function(app, sn, output, prop, value)
            self._log:notice('on_output', app, sn, output, prop, value)
            local s7dev = self._devsyy[sn][1]
            local s7stat = self._devsyy[sn][2]
            local s7client = self._devsyy[sn][3]
            local outputs = self._devsyy[sn][4]
            local devsn = self._devsyy[sn][5]
            local now = self._sys:time()
            for i, v in ipairs(outputs) do
                -- self._log:notice("ouput name: ", v.name)
                if output == v.name then
                -- self._log:notice("this is target tagname: ", v.name)

                    if not s7dev._s7link_status then
                        if not s7dev._s7link_error then
                            self._log:info(devsn .. " 连接断开!")
                            s7dev:dump_comm('link', 'disconnected')
                            s7dev:fire_event(event.LEVEL_FATAL, event.EVENT_COMM, devsn .. "/" .. "连接断开!", { os_time = os.time(), time = now })
                            s7dev._s7link_error = true
                            s7dev._s7link_error_time = now
                        elseif (now - s7dev._s7link_error_time) > 3600 then
                            s7dev:fire_event(event.LEVEL_FATAL, event.EVENT_COMM, devsn .. "/" .. "连接断开!", { os_time = os.time(), time = now })
                            s7dev._s7link_error_time = now
                        end

                    else
                        s7dev._s7link_error = false
                        -- WriteArea(int Area, int DBNumber, int Start, int Amount, int WordLen, void *pUsrData);
                        self._log:info("FCFCFCFC:::", v.fc, v.dt)
                        local tag_S7AreaID = nil
                        local tag_S7WordLen = s7.S7WordLen.Word
                        local tag_S7Addr = v.saddr
                        local Amount = _dt_len_map[v.dt]
                        if v.fc == "I" then
                            tag_S7AreaID = s7.S7AreaID.PE
                        end
                        if v.fc == "Q" then
                            tag_S7AreaID = s7.S7AreaID.PA
                        end
                        if v.fc == "M" then
                            tag_S7AreaID = s7.S7AreaID.MK
                        end
                        if v.fc == "DB" then
                            tag_S7AreaID = s7.S7AreaID.DB
                        end
                        if v.fc == "CT" then
                            tag_S7AreaID = s7.S7AreaID.CT
                        end
                        if v.fc == "TM" then
                            tag_S7AreaID = s7.S7AreaID.TM
                        end
                        
                        local data, str

                        if v.dt == "string" then
                            str = value
                            tag_S7WordLen = s7.S7WordLen.Byte
                            Amount = math.ceil(v.pos/2)
                            data = s7.UserData.new(str, v.pos)
                        elseif v.dt == "bool" then
                            tag_S7WordLen = s7.S7WordLen.Bit
                            tag_S7Addr = (v.saddr * 8) + v.pos
                            if value == true then
                                value = 1
                            elseif value == false then
                                value = 0
                            end
                            str = string.pack(">I1", tonumber(value))
                            data = s7.UserData.new(str, 1)
                        else
                            if v.dt ~= "float" then
                                value = math.ceil(value)
                            end
                            str = string.pack(_dt_format[v.dt], value)
                            data = s7.UserData.new(str, _byte_len_map[v.dt])
                        end
                        self._log:info("Start set value:", value)
                        -- self._log:info("hexdata:", basexx.to_hex(str))                        


                        if tag_S7AreaID == nil then
                            return false, "tag_S7AreaID is nil"
                        end
                        self._log:info("write info::::", tag_S7AreaID, v.dbnum, tag_S7Addr, Amount, tag_S7WordLen)
                        local r_ret = s7client:WriteArea(tag_S7AreaID, v.dbnum, tag_S7Addr, Amount, tag_S7WordLen, data.data);

                        if r_ret ~= 0 then
                            self._log:info(v.name .. " Write Failed!", r_ret)
                            s7dev:dump_comm('write', v.name ..' Write Failed err: ' .. s7.CliErrorText(r_ret))
                            self._log:info(s7.CliErrorText(r_ret))
                            return false, s7.CliErrorText(r_ret)
                        else
                            self._log:info(v.name .. " Write successful", r_ret)
                            s7dev:dump_comm('write', v.name ..' Write successful')
                            s7dev:fire_event(event.LEVEL_INFO, event.EVENT_COMM, sn .. "/" .. v.name .. "/" .. "设置数值成功!", { os_time = os.time(), time = now })
                            s7dev:set_input_prop_emergency(v.name, "value", value, self._sys:time(), 0)
                            return true
                        end
                    end
                end
            end
        end,
        on_command = function(app, sn, command, param)
        end,
        on_ctrl = function(app, command, param, ...)
        end,
    })

    --- 生成设备唯一序列号
    local sys_id = self._sys:id()
    
    self._cycle = 1000
    if self._conf.cycle ~= nil then
        if math.tointeger( tonumber(self._conf.cycle)) > 0 then
            self._cycle = math.tointeger( tonumber(self._conf.cycle))
        end
    end
    
    local Link = self._conf

    if Link == nil then
        Link = require 'userlib.linkcfg'
    else
        if next(Link) == nil then
            Link = require 'userlib.linkcfg'
        end
    end

    local helper = conf_helper:new(self._sys, Link)
    helper:fetch()
    
	self._devs = {}
    for i, v in ipairs(helper:devices()) do
		assert(v.sn and v.name and v.tpl)
        self._log:notice("@@@@@@@@@"..i, v.sn , v.name , v.tpl)
	end
    local Link = helper:config()

    local devinfo = require 'userlib.linkcfg'
    local devsxx = {}
    local devsyy = {}

    --- 增加设备实例
    for i, dev in ipairs(Link.devs) do
        -- self._log:notice("-------------",dev.sn, dev.name, dev.addr)
        local s7client = s7.TS7Client.new()
        --- 加载CSV点表并分析
        csv_tpl.init(self._sys:app_dir())
        local tplfile = dev.tpl
        local tpl, err = csv_tpl.load_tpl(tplfile)
        local _points = {}
        local outputs = {}
        if not tpl then
            self._log:error("loading csv tpl failed", err)
        else
            _points = tpl.inputs
            outputs = tpl.outputs
        end
        if #outputs > 0 then
            table.move(outputs, 1, #outputs, #_points + 1, _points)
        end
        for m, n in ipairs(_points) do
            if string.upper(n.rw) == string.upper("rw") then
                table.insert(outputs, n)
            end
        end
        -- table.move(devinfo.inputs, 1, #devinfo.inputs, #_points+1, _points)
        --- 加载CSV点表并分析
        local items_g = {}
        local items = {}
        local data_items = {}
        for i, v in ipairs(_points) do
            local item = s7.TS7DataItem.new()
            -- self._log:info(i, v.name, 'rate:', v.rate)
            local data
            if v.dt == "string" and v.pos>0 then
                data = s7.UserData.new(v.pos)
            else
                if v.dt ~= nil then
                    local datalen = _dt_len_map[v.dt]
                    if datalen ~= nil then
                        data = s7.UserData.new(datalen)
                    end
                end
            end
            item.Start = v.saddr
            item.DBNumber = v.dbnum
            
            item.pdata = data.data
            if v.fc == "I" then
                item.Area = s7.S7AreaID.PE
            end
            if v.fc == "Q" then
                item.Area = s7.S7AreaID.PA
            end
            if v.fc == "M" then
                item.Area = s7.S7AreaID.MK
            end
            if v.fc == "DB" then
                item.Area = s7.S7AreaID.DB
            end
            if v.fc == "CT" then
                item.Area = s7.S7AreaID.CT
            end
            if v.fc == "TM" then
                item.Area = s7.S7AreaID.TM
            end
            
            if v.dt == "bool" then
                item.WordLen = s7.S7WordLen.Bit
                item.Start = (v.saddr * 8) + v.pos
                item.Amount = _dt_len_map[v.dt]
            elseif v.dt == "string" then
                item.WordLen = s7.S7WordLen.Byte
                item.Amount = math.ceil(v.pos/2)
            else
                item.WordLen = s7.S7WordLen.Word
                item.Amount = _dt_len_map[v.dt]
            end

            if #items > 10 then
                -- self._log:info(i)
                items_g[#items_g + 1] = { items, data_items }
                items = {}
                data_items = {}
                items[#items + 1] = item
                local dataobj = {}
                dataobj["item"] = item
                dataobj["data"] = data
                dataobj["tagname"] = v.name
                dataobj["rate"] = v.rate
                if v.dt == "string" then
                    dataobj["format"] = v.dt
                    dataobj["bytelen"] = v.pos
                else
                    dataobj["format"] = _dt_format[v.dt]
                    dataobj["bytelen"] = _byte_len_map[v.dt] * _dt_len_map[v.dt]
                end

                data_items[#data_items + 1] = dataobj
            else
                items[#items + 1] = item
                local dataobj = {}
                dataobj["item"] = item
                dataobj["data"] = data
                dataobj["tagname"] = v.name
                dataobj["rate"] = v.rate
                if v.dt == "string" then
                    dataobj["format"] = v.dt
                    dataobj["bytelen"] = v.pos
                else
                    dataobj["format"] = _dt_format[v.dt]
                    dataobj["bytelen"] = _byte_len_map[v.dt] * _dt_len_map[v.dt]
                end
                data_items[#data_items + 1] = dataobj
            end

        end
        items_g[#items_g + 1] = { items, data_items }
        
        --- 设备的参数
        local plc_host = dev.host
        local Local_T = dev.local_tsap or '0200'
        local remote_T = dev.remote_tsap or '0200'
        local Local_TSAP = tonumber(Local_T, 16)
        local remote_TSAP = tonumber(remote_T, 16)
        --- 生成设备的序列号
        local dev_sn = sys_id .. "." .. self._name .. "." .. dev.sn
        --- 生成设备对象
        local meta = self._api:default_meta()
        if (dev.name ~= nil) then
            meta.inst = dev.name
        else
            meta.inst = devinfo.meta.name
        end
        if (dev.desc ~= nil) then
            meta.description = dev.desc
        else
            meta.description = devinfo.meta.description
        end
        meta.series = devinfo.meta.series
        meta.manufacturer = devinfo.meta.manufacturer

        local s7dev = self._api:add_device(dev_sn, meta, _points, outputs)

        --- 设备对象设置初值，IO点数值为0，质量戳为99
        -- self._log:debug(dev_sn, "设置初值")
        for m, n in ipairs(_points) do
            s7dev:set_input_prop(n.name, "value", 0, self._sys:time(), 99)
        end
        -- self._log:debug("连接信息：：",dev_sn,plc_host, Local_TSAP, remote_TSAP)
        s7dev._s7link_status = false
        s7client:SetConnectionParams(plc_host, Local_TSAP, remote_TSAP)
        local connect_ret = s7client:Connect()
        self._log:info("Connect:\t", connect_ret)
        if not s7client:Connected() then
            self._log:info(s7.CliErrorText(connect_ret))
            s7dev:dump_comm('link', 'disconnected')
            s7client:Connect()
            if s7client:Connected() then
                s7dev:dump_comm('link', 'connected')
                s7dev._s7link_status = true
            end
        else
            self._log:info("s7client", s7client:Connected())
            s7dev:dump_comm('link', 'connected')
            s7dev._s7link_status = true
            -- local cpuinfo = s7.TS7CpuInfo.new()
            -- local ret1 = s7client:GetCpuInfo(cpuinfo)
            -- if ret1 == 0 then
            --     s7dev:set_input_prop("ModuleTypeName", "value", cpuinfo.ModuleTypeName, self._sys:time(), 0)
            --     s7dev:set_input_prop("SerialNumber", "value", cpuinfo.SerialNumber, self._sys:time(), 0)
            --     s7dev:set_input_prop("ASName", "value", cpuinfo.ASName, self._sys:time(), 0)
            --     s7dev:set_input_prop("ModuleName", "value", cpuinfo.ModuleName, self._sys:time(), 0)
            --     s7dev:set_input_prop("PlcStatus", "value", s7client:PlcStatus(), self._sys:time(), 0)
            -- end
        end

        --- 生成设备通讯口统计对象
        local devstat = s7dev:stat('port')

        local devobj = {}
        devobj["dev"] = s7dev
        devobj["items_g"] = items_g
        devobj["s7client"] = s7client
        devobj["devsn"] = dev_sn
        devobj["stat"] = devstat
        devobj["event_trigger"] = {}
        devsyy[dev_sn] = { s7dev, devstat, s7client, tpl.outputs, dev.sn }
        table.insert(devsxx, devobj)
    end
    self._devsxx = devsxx
    self._devsyy = devsyy

    return true
end

--- 应用退出函数
function app:close(reason)
    self._log:notice(self._name, reason)
end

--- 应用运行入口
function app:run(tms)
    for _, dev in ipairs(self._devsxx) do
        local s7client = dev.s7client
        local s7dev = dev.dev
        local devsn = dev.devsn
        local s7stat = dev.stat
        local items_g = dev.items_g
        local now = self._sys:time()
        
        -- local PlcStatus
        -- local ret1 = s7client:PlcStatus()
        -- self._log:info("PlcStatus::", ret1)
        -- if (ret1 & 0x0000FFFF) ~= 0 then
        --     s7dev._s7link_status = false
        -- else
        --     s7dev._s7link_status = true
        -- end
            
        if not s7dev._s7link_status then
            if not s7dev._s7link_error then
                self._log:info(devsn .. " 连接断开!")
                s7dev:dump_comm('link', 'disconnected')
                s7dev:fire_event(event.LEVEL_FATAL, event.EVENT_COMM, devsn .. "/" .. "连接断开!", { os_time = os.time(), time = now })
                s7dev._s7link_error = true
                s7dev._s7link_error_time = now
            elseif (now - s7dev._s7link_error_time) > 3600 then
                s7dev:fire_event(event.LEVEL_FATAL, event.EVENT_COMM, devsn .. "/" .. "连接断开!", { os_time = os.time(), time = now })
                s7dev._s7link_error_time = now
            end
            for _, g in ipairs(items_g) do
                local items = g[1]
                local data_items = g[2]
                for i, v in ipairs(data_items) do
                    local lastvalue = s7dev:get_input_prop(v["tagname"], "value")
                    s7dev:set_input_prop(v["tagname"], "value", lastvalue, self._sys:time(), 1)
                end
            end

            local connect_ret = s7client:Connect()
            -- self._log:debug("info:", s7.CliErrorText(connect_ret))
            if s7client:Connected() then
                s7dev:dump_comm('link', 'connected')
                s7dev._s7link_status = true
            end
            dev.s7client = s7client
        else
            -- self._log:debug("s7clients7", s7client:Connected())
            
            s7dev._s7link_error = false
            for _, g in ipairs(items_g) do

                local items = g[1]
                local data_items = g[2]
                local ret = s7client:ReadMultiVars(items, #items)
                -- self._log:debug('ReadMultiVars', ret)
                if (ret & 0x0000FFFF) ~= 0 then
                    s7dev._s7link_status = false
                    -- self._log:info("连接异常!")
                else
                    s7dev._s7link_status = true
                    -- self._log:info("连接正常!")
                end
                if s7dev._s7link_status then
                    if ret ~= 0 then
                        -- self._log:info(s7.CliErrorText(ret))
                        s7dev:dump_comm('read', 'failed, err:' .. s7.CliErrorText(ret))
                    else
                        s7dev:dump_comm('read', 'successful')                
                    
                        for i, v in ipairs(data_items) do
                            -- self._log:debug("Item return:"..i, v["item"].Area, v["item"].Start, v["item"].WordLen, v["item"].Result)
                            if v["item"].Result == 0 then
                                -- self._log:debug("data hex:"..i, basexx.to_hex(v["data"]:str(v["bytelen"])))
                                local val, index = nil, nil
                                if v["format"]=="string" then
                                    val, index = string.unpack('z', v["data"]:str(v["bytelen"]))
                                    -- self._log:debug("data hex:", v["tagname"], val)
                                    local r, err = s7dev:set_input_prop(v["tagname"], "value", val, self._sys:time(), 0)
                                    -- self._log:debug(v["tagname"], r, err)
                                else
                                    val, index = string.unpack(v["format"], v["data"]:str(v["bytelen"]))
                                    -- self._log:debug(v["format"], v["tagname"], val)
                                    s7dev:set_input_prop(v["tagname"], "value", val * v["rate"], self._sys:time(), 0)
                                end
        
                                -- self._log:debug(v["tagname"].." data:"..i, val)
                            else
                                self._log:info(v["tagname"], v["item"].Result)
                            end
                        end
                    end
                end
            end

        end
        -- dev:set_input_prop('tag1', "value", math.random())
    end

    return self._cycle
end

--- 返回应用对象
return app