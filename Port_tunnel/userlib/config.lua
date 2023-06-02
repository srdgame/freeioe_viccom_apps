local cfg = {
    enable_adminapi = 1,
    tcp_devices = {
        {
            id = "device_01",
            dest_ip = "192.168.0.114",
            dest_port = "3389"
        }
    }

}

return cfg