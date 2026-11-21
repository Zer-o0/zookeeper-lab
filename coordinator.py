"""Координатор расчёта частот навыков из ДЗ 3 (вакансии Java: одиночные навыки, пары, тройки)
с выбором лидера через ZooKeeper, чекпоинтом и публикацией результата в указатель /hw4/result.

Запуск:  python coordinator.py a      (вторая копия: python coordinator.py b)
Переменные окружения: RUN - имя запуска (для журнала и чекпоинта), FENCING=1 - проверка эпохи при публикации.
"""
import csv
import hashlib
import json
import os
import pickle
import re
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import combinations, islice
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss, SessionExpiredError

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv('DATA', ROOT.parent / 'datainj' / 'data' / 'it_vacancies_2024.csv'))
HW3_RESULT = Path(os.getenv('HW3_RESULT', ROOT.parent / 'datainj' / 'results' / 'mapreduce.json'))
HOSTS = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
SESSION_TIMEOUT = 10.0
BASE = '/hw4'
RUN = os.getenv('RUN', 'run')
FENCING = os.getenv('FENCING') == '1'
NAME = sys.argv[1] if len(sys.argv) > 1 else 'a'
CHECKPOINT = ROOT / 'checkpoints' / f'{RUN}.pkl'
LOG = ROOT / 'logs' / f'{RUN}-{NAME}.log'

# Параметры расчёта - как в ДЗ 3 (datainj/lab.py)
ROLE = 'Программист, разработчик'
PATTERN = r'(^|[^a-z0-9а-яё])java([^a-z0-9а-яё]|$)'
CHUNK_ROWS = 2000
WORKERS = 2


def log(message):
    line = f'{datetime.now():%Y-%m-%d %H:%M:%S.%f}'[:-3] + f' [{NAME}] {message}'
    print(line, flush=True)
    with LOG.open('a', encoding='utf-8') as f:
        f.write(line + '\n')


def map_chunk(rows):  # без изменений из ДЗ 3
    counts, sizes, ids = Counter(), Counter(), []
    for row in rows:
        if row['role'] != ROLE or not re.search(PATTERN, row['name'], re.I):
            continue
        ids.append(row['source'] + ':' + row['vacancy_id'])
        skills = sorted({s.strip().lower() for s in row['key_skills'].split(';') if s.strip()})
        sizes[len(skills)] += 1
        for k in (1, 2, 3):
            counts.update(combinations(skills, k))
    return counts, sizes, ids


def zk_call(fn, *args, **kwargs):
    """Повтор операции, пока клиент переподключается (в т.ч. новой сессией после истечения старой)."""
    for _ in range(120):
        try:
            return fn(*args, **kwargs)
        except (ConnectionLoss, SessionExpiredError):
            time.sleep(0.5)
    return fn(*args, **kwargs)


def save_checkpoint(state):
    tmp = CHECKPOINT.with_suffix('.tmp')
    tmp.write_bytes(pickle.dumps(state))
    os.replace(tmp, CHECKPOINT)  # атомарная замена: файл чекпоинта всегда целый


def compute():
    if CHECKPOINT.exists():
        state = pickle.loads(CHECKPOINT.read_bytes())
        log(f'продолжаю с чекпоинта: чанков {state["chunks"]}, строк {state["rows_read"]}')
    else:
        state = dict(rows_read=0, chunks=0, total=Counter(), sizes=Counter(), ids=[])
        log('чекпоинта нет, начинаю расчёт с начала')
    csv.field_size_limit(100_000_000)
    with DATA.open(encoding='utf-8-sig', newline='') as text, ProcessPoolExecutor(max_workers=WORKERS) as pool:
        reader = csv.DictReader(text)
        for _ in islice(reader, state['rows_read']):  # пропуск уже обработанных строк
            pass
        while True:
            batch = []
            for _ in range(WORKERS * 2):
                rows = list(islice(reader, CHUNK_ROWS))
                if not rows:
                    break
                state['rows_read'] += len(rows)
                batch.append([{k: r[k] for k in ('source', 'vacancy_id', 'name', 'role', 'key_skills')} for r in rows])
            if not batch:
                break
            for future in [pool.submit(map_chunk, rows) for rows in batch]:
                counts, hist, selected = future.result()
                state['total'].update(counts)
                state['sizes'].update(hist)
                state['ids'].extend(selected)
                state['chunks'] += 1
            save_checkpoint(state)
            log(f'чекпоинт: чанков {state["chunks"]}, строк {state["rows_read"]}')
    counts = {str(k): [dict(skills=list(s), frequency=n) for s, n in sorted(state['total'].items()) if len(s) == k]
              for k in (1, 2, 3)}
    return counts, sorted(state['ids'])


def lead(zk):
    log('стал лидером')
    epoch = zk.set(f'{BASE}/epoch', NAME.encode()).version  # каждая смена лидера увеличивает эпоху
    log(f'эпоха {epoch}')
    if zk.get(f'{BASE}/result')[0]:
        log('результат уже опубликован, расчёт не нужен')
        return
    start = time.perf_counter()
    counts, ids = compute()
    log(f'расчёт закончен за {time.perf_counter() - start:.1f} с')
    control = [f'{len(ids)} Java vacancies'] + [
        f'k={k}: unique={len(rows)}, occurrences={sum(r["frequency"] for r in rows)}' for k, rows in counts.items()]
    for line in control:
        log('контроль: ' + line)
    hw3 = json.loads(HW3_RESULT.read_text(encoding='utf-8'))
    log(f'совпадает с ДЗ 3 (полные словари частот и id вакансий): {counts == hw3["counts"] and ids == hw3["ids"]}')
    digest = hashlib.sha256(json.dumps(counts, ensure_ascii=False).encode()).hexdigest()
    payload = json.dumps(dict(leader=NAME, epoch=epoch, control=control, counts_sha256=digest),
                         ensure_ascii=False).encode()
    if FENCING:
        def commit():  # условная запись: пройдёт, только если эпоха не сменилась
            tx = zk.transaction()
            tx.check(f'{BASE}/epoch', epoch)
            tx.set_data(f'{BASE}/result', payload)
            return tx.commit()
        check, written = zk_call(commit)
        if isinstance(check, Exception):
            current = zk_call(zk.get, f'{BASE}/epoch')[1].version
            log(f'запись отклонена: {type(check).__name__} (моя эпоха {epoch}, текущая {current})')
            return
        version = written.version
    else:
        version = zk_call(zk.set, f'{BASE}/result', payload).version
    log(f'записал результат: эпоха {epoch}, версия указателя {version}')


def main():
    LOG.parent.mkdir(exist_ok=True)
    CHECKPOINT.parent.mkdir(exist_ok=True)
    zk = KazooClient(hosts=HOSTS, timeout=SESSION_TIMEOUT)
    zk.add_listener(lambda state: log(f'сессия ZooKeeper: {state}'))
    zk.start()
    for path in ('epoch', 'result', 'election'):
        zk.ensure_path(f'{BASE}/{path}')
    log(f'запущен, FENCING={int(FENCING)}, жду лидерства')
    zk.Election(f'{BASE}/election', NAME).run(lead, zk)
    zk.stop()
    log('завершён')


if __name__ == '__main__':
    main()
