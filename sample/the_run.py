# -*- coding: utf-8 -*-
import argparse
import os
import subprocess
import time

DEBUG_LEN = 2
REPLAY_LEN = 10


def convert(arr, name, length):
    if arr is None:
        name = ""
        arr = ["" for i in range(length)]
    else:
        arr = list(map(str, arr))
        arr += ["" for i in range(length - len(arr))]

    return [name] + arr


def main(interactor, data, player, debug, replay):
    data_out = "result.txt"

    debug = convert(debug, "debug", DEBUG_LEN)
    replay = convert(replay, "replay", REPLAY_LEN)

    pipe1_read, pipe1_write = os.pipe()
    pipe2_read, pipe2_write = os.pipe()

    process1 = subprocess.Popen([interactor, data, data_out] + debug + replay,
                                stdin=pipe2_read,
                                stdout=pipe1_write)

    process2 = subprocess.Popen(player.split(),
                                stdin=pipe1_read,
                                stdout=pipe2_write)

    process1.wait()
    time.sleep(0.1)
    process2.terminate()
    time.sleep(0.1)

    os.close(pipe1_read)
    os.close(pipe1_write)
    os.close(pipe2_read)
    os.close(pipe2_write)


if __name__ == '__main__':
    # 直接在此处设置参数
    
    complex_test = 'data/sample_practice.in'
    simple_test = 'data/sample.in'
    
    demo = 'records/demo.exe'
    my = 'records/main.exe'
    
    args = argparse.Namespace(
        interactor=['interactor/linux/interactor'],  # 注意保持列表格式
        data=[complex_test],
        player=[my],
        debug= [1],        # 调试参数（可选）
        replay= []        # 回放参数（可选）
    )

    os.makedirs("replay", exist_ok=True)
    main(args.interactor[0], args.data[0], args.player[0], args.debug, args.replay)
