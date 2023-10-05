import sys
import os
import asyncio
import json
import datetime
from uuid import uuid4
from websockets.client import connect
from aiortc import RTCPeerConnection, RTCConfiguration
from aiortc.contrib.signaling import object_to_string, object_from_string
from .files import FileReader, FileWriter
from .protocol import XferSender, XferReceiver


url = 'wss://transfer.zip/ws'
receiver_id = ''


async def consume_signaling(pc, ws):
    global receiver_id
    while True:
        rcv = await ws.recv()
        message = json.loads(rcv)
        if message.get('type') == 12: # answer
            remote_des = object_from_string(message.get('answer'))
            await pc.setRemoteDescription(remote_des)

        elif message.get('type') == 11: # offer
            if 'id' in message.get('offer'): # id exchange, send real offer
                await pc.setLocalDescription(await pc.createOffer())
                real_offer = {
                    'type': 1,
                    'offer': {
                        'description': object_to_string(pc.localDescription)
                    },
                    'recipientId': message.get('callerId')
                }
                receiver_id = message.get('callerId')
                await ws.send(json.dumps(real_offer))

            elif 'description' in message.get('offer'):
                remote_des = object_from_string(message.get('offer').get('description'))
                await pc.setRemoteDescription(remote_des)
                await pc.setLocalDescription(await pc.createAnswer())
                answer = {
                    'type': 2,
                    'answer': object_to_string(pc.localDescription),
                    'recipientId': message.get('callerId')
                }
                await ws.send(json.dumps(answer))

            elif 'bye' in message.get('offer'):
                print('receive end signal')
                break
        else:
            if message.get('success') != True:
                print('Error when signaling:\n', message, datetime.datetime.now())


async def run_receiver(pc, peerId, basedir):
    uuid = uuid4()
    receiver = XferReceiver()
    login = {
            'type': 0,
            'id': str(uuid)
        }
    offer = {
            'type': 1,
            'offer': {
                'id': str(uuid)
            },
            'recipientId': peerId
        }
    bye_signal = {
            'type': 1,
            'offer': {
                'bye': 'BYEBYE'
            },
            'recipientId': peerId
        }

    websocket = await connect(url)
    fwriter = FileWriter(basedir)

    @pc.on('datachannel')
    def on_datachannel(channel):
        @channel.on('message')
        async def on_message(message):
            if message:
                for obj in receiver.receive_from_bytes(message):
                    if isinstance(obj, dict): # meta
                        fwriter.create(obj)
                    elif isinstance(obj, bytes):
                        fwriter.write_reg(obj)

            else:
                print('receive done')
                await websocket.send(json.dumps(bye_signal))
                sys.exit(0)

    await websocket.send(json.dumps(login))
    await websocket.send(json.dumps(offer))
    await consume_signaling(pc, websocket)

async def run_sender(pc, send_path):
    uuid = uuid4()
    print(uuid)
    channel = pc.createDataChannel('filexfer')
    reader = FileReader(send_path)
    sender = XferSender(16384, reader)
    data_generator = sender.data_segments()
    done = False

    websocket = await connect(url)

    async def send_data():
        nonlocal done
        while (channel.bufferedAmount <= channel.bufferedAmountLowThreshold) and not done:
            try:
                data = next(data_generator)
                channel.send(data)
            except StopIteration:
                print('send complete')
                channel.send(bytes())
                done = True

    channel.on('open', send_data)
    channel.on("bufferedamountlow", send_data)

    login = {
            'type': 0,
            'id': str(uuid)
            }

    await websocket.send(json.dumps(login))
    await consume_signaling(pc, websocket)

def print_help():
    print('sender: filexfer dir/file\nreceiver: filexfer uuid [receive_path]')

def main():
    peerId = ''
    send_path = ''
    role = ''
    recv_dir = '.'
    if os.path.exists(sys.argv[1]):
        role = 'send'
        send_path = sys.argv[1]
    else:
        role = 'rcv'
        peerId = sys.argv[1]

    if len(sys.argv) == 3:
        if os.path.exists(sys.argv[2]) and role == 'rcv':
            recv_dir = sys.argv[2]
        else:
            print_help()
            sys.exit(2)

    if role == 'send' and len(sys.argv) != 2:
        print_help()
        sys.exit(2)

    config = RTCConfiguration([])
    pc = RTCPeerConnection(config)
    if role == 'rcv':
        coro = run_receiver(pc, peerId, recv_dir)
    else:
        coro = run_sender(pc, send_path)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
