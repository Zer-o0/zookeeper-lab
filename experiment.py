"""Этапы 3 и 4: две копии координатора, приостановка лидера посреди расчёта.

python experiment.py stage3   # без защиты (FENCING=0)
python experiment.py stage4   # с защитой по эпохе (FENCING=1)
"""
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import psutil
from kazoo.client import KazooClient

ROOT = Path(__file__).resolve().parent
STAGE = sys.argv[1]
RUN = {'stage3': 'stage3-no-fencing', 'stage4': 'stage4-fencing'}[STAGE]
FENCING = '1' if STAGE == 'stage4' else '0'
PAUSE_AFTER_CHUNKS = 56   # примерно половина из 115 чанков
MIN_PAUSE = 40            # секунд
OUT = ROOT / 'outputs' / f'{RUN}.txt'


def say(message):
    line = f'{datetime.now():%Y-%m-%d %H:%M:%S.%f}'[:-3] + f' [experiment] {message}'
    print(line, flush=True)
    with OUT.open('a', encoding='utf-8') as f:
        f.write(line + '\n')


def log_lines(name):
    path = ROOT / 'logs' / f'{RUN}-{name}.log'
    return path.read_text(encoding='utf-8').splitlines() if path.exists() else []


def wait_for(name, predicate, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        for line in log_lines(name):
            if predicate(line):
                return line
        time.sleep(0.2)
    raise TimeoutError(f'{name}: нет ожидаемой строки в журнале')


def stamp(line):
    return datetime.strptime(line[:23], '%Y-%m-%d %H:%M:%S.%f')


def tree(pid):  # на Windows python.exe из venv запускает настоящий интерпретатор дочерним процессом
    p = psutil.Process(pid)
    return [p] + p.children(recursive=True)


def main():
    OUT.parent.mkdir(exist_ok=True)
    OUT.unlink(missing_ok=True)
    for name in 'ab':
        (ROOT / 'logs' / f'{RUN}-{name}.log').unlink(missing_ok=True)
    (ROOT / 'checkpoints' / f'{RUN}.pkl').unlink(missing_ok=True)
    zk = KazooClient(hosts='127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183')
    zk.start()
    zk.delete('/hw4', recursive=True) if zk.exists('/hw4') else None
    say(f'{RUN}: /hw4 и чекпоинт очищены, FENCING={FENCING}')

    env = dict(os.environ, RUN=RUN, FENCING=FENCING, PYTHONIOENCODING='utf-8')
    procs = {}
    leader, backup = 'a', 'b'
    for name, ready in ((leader, 'стал лидером'), (backup, 'жду лидерства')):
        procs[name] = subprocess.Popen([sys.executable, 'coordinator.py', name], cwd=ROOT, env=env,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        wait_for(name, lambda l: ready in l)
        say(f'запущен координатор {name}, pid {procs[name].pid}: {ready}')

    wait_for(leader, lambda l: 'чекпоинт: чанков' in l and int(l.split('чанков ')[1].split(',')[0]) >= PAUSE_AFTER_CHUNKS)
    paused = tree(procs[leader].pid)
    for p in paused:
        p.suspend()
    t_pause = datetime.now()
    say(f'лидер {leader} приостановлен посреди расчёта')

    elected = wait_for(backup, lambda l: 'стал лидером' in l)
    say(f'смена лидера: {backup} стал лидером через {(stamp(elected) - t_pause).total_seconds():.1f} с после приостановки')
    procs[backup].wait(timeout=600)
    say(f'запасной {backup} завершил работу')
    left = MIN_PAUSE - (datetime.now() - t_pause).total_seconds()
    if left > 0:
        time.sleep(left)
    for p in paused:
        p.resume()
    say(f'лидер {leader} возобновлён, пауза {(datetime.now() - t_pause).total_seconds():.1f} с')
    procs[leader].wait(timeout=600)
    say(f'бывший лидер {leader} завершил работу')

    data, stat = zk.get('/hw4/result')
    say(f'указатель /hw4/result: версия {stat.version}, данные {data.decode()}')
    zk.stop()
    with OUT.open('a', encoding='utf-8') as f:
        for name in 'ab':
            f.write(f'\n===== logs/{RUN}-{name}.log =====\n' + '\n'.join(log_lines(name)) + '\n')


if __name__ == '__main__':
    main()
