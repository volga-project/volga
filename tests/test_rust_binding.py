from volga_rust import Channel, LocalChannel, RemoteChannel

c = Channel('ch_0')
lc = LocalChannel('ch_0', 'ipc_addr_0')

print(lc.channel_id, lc.ipc_addr)
print(isinstance(lc, Channel))
# rc = RemoteChannel()
