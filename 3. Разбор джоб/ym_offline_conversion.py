"""ClickHouse -> сохранённая очередь GP -> Яндекс.Метрика. Импорт не запускает обработку."""
import hashlib
import argparse
import logging
import re
import os
from uuid import uuid4
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import clickhouse_connect
import pandas as pd
import psycopg
import requests
try:
    from dwh_libs.dwh_logger import dwh_logger as logger
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger('ym_offline_conversion')

TABLE = 'team_osago.ym_offline_conversion'
LOG = 'team_osago.ym_offline_conversion_log'
COLUMNS = ['event_id', 'client_id', 'target', 'datetime', 'price', 'currency',
           'order_id', 'sale_date', 'source_medium', 'utm_campaign']
UPLOAD_COLUMNS = ['ClientId', 'Target', 'DateTime', 'Price', 'Currency']
RENAME = dict(zip(COLUMNS[1:6], UPLOAD_COLUMNS))
MSK = ZoneInfo('Europe/Moscow')


def make_event_id(row):
    # Сохраняем прежний ключ и представление пустых значений для совместимости дедупликации.
    values = [row.order_id, str(row.sale_date), row.source_medium, row.utm_campaign]
    serialized = ''.join('-1:' if pd.isna(v) else f'{len(str(v))}:{v}' for v in values)
    return f'order_day_campaign_{hashlib.md5(serialized.encode()).hexdigest()}_{row.target}'


def require_env(names):
    missing = [name for name in names if not os.getenv(name, '').strip()]
    if missing:
        raise ValueError(f'Не заданы переменные: {", ".join(missing)}')


def metrica_error_details(response, token):
    """Возвращает только диагностические поля без заголовков, данных запроса и полного тела ответа."""
    try:
        body = response.json()
    except ValueError:
        return 'Ответ не в формате JSON; тело ответа скрыто'
    if not isinstance(body, dict):
        return 'Неожиданный формат JSON; тело ответа скрыто'
    parts = []
    for key in ('code', 'message'):
        if isinstance(body.get(key), (str, int)):
            parts.append(f'{key}={body[key]}')
    errors = body.get('errors', [])
    if isinstance(errors, list):
        for error in errors[:5]:
            if isinstance(error, dict):
                for key in ('error_type', 'message'):
                    if isinstance(error.get(key), str):
                        parts.append(f'{key}={error[key]}')
    details = '; '.join(parts) or 'Диагностические поля отсутствуют'
    if token:
        details = details.replace(token, '[TOKEN REDACTED]')
    details = re.sub(r'(?i)\b(?:OAuth|Bearer)\s+\S+', '[AUTH REDACTED]', details)
    return ' '.join(details.split())[:1500]


def connect_gp(local=False):
    if not local:
        from dwh_libs.workflows.shared_scripts import set_dwh_envs
        from vault_envs import load_connection_envs
        set_dwh_envs()
        load_connection_envs()
    require_env(['GP_HOST', 'GP_PORT', 'GP_DATABASE', 'GP_USER', 'GP_PASSWORD'])
    return psycopg.connect(host=os.environ['GP_HOST'], port=os.environ['GP_PORT'],
                           dbname=os.environ['GP_DATABASE'], user=os.environ['GP_USER'],
                           password=os.environ['GP_PASSWORD'], options='-c timezone=Europe/Moscow')


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
            run_id text NOT NULL, event_id text NOT NULL, client_id text NOT NULL,
            target text NOT NULL, datetime bigint NOT NULL, price numeric(24,6),
            currency varchar(3) NOT NULL, order_id text NOT NULL, sale_date date NOT NULL,
            source_medium text, utm_campaign text,
            loaded_at timestamptz NOT NULL DEFAULT current_timestamp
        ) DISTRIBUTED BY (event_id)''')
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {LOG} (
            event_id text NOT NULL, client_id text NOT NULL, target text NOT NULL,
            datetime bigint NOT NULL, price numeric(24,6),
            uploaded_at timestamptz NOT NULL DEFAULT current_timestamp
        ) DISTRIBUTED BY (event_id)''')
        # В прежнем журнале были только успешные отправки: существующие строки считаем ACCEPTED.
        # Старые версии Greenplum не поддерживают ADD COLUMN IF NOT EXISTS.
        # При параллельных запусках проверяем и меняем структуру таблицы последовательно.
        cur.execute(f'LOCK TABLE {LOG} IN ACCESS EXCLUSIVE MODE')
        cur.execute('''SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s''',
                    ('team_osago', 'ym_offline_conversion_log'))
        existing = {row[0] for row in cur.fetchall()}
        for name, definition in (
            ('status', "text NOT NULL DEFAULT 'ACCEPTED'"),
            ('upload_id', 'bigint'),
            ('batch_id', 'text'),
        ):
            if name not in existing:
                cur.execute(f'ALTER TABLE {LOG} ADD COLUMN {name} {definition}')
    conn.commit()


def resolve_period(date_from='', date_to=''):
    today = datetime.now(MSK).date()
    start = date.fromisoformat(date_from) if date_from else today - timedelta(days=1)
    end = date.fromisoformat(date_to) if date_to else today
    if start > end or end > today:
        raise ValueError('Ожидается date_from <= date_to <= сегодня по Москве')
    return start, end + timedelta(days=1)


def load_conversions(run_id, date_from='', date_to='', *, local=False):
    start, until = resolve_period(date_from, date_to)
    with connect_gp(local=local) as conn:
        ensure_tables(conn)
        require_env(['CH_HOST', 'CH_PORT', 'CH_USER', 'CH_PASSWORD', 'CH_SECURE'])
        secure = os.environ['CH_SECURE'].strip().lower()
        if secure not in {'true', 'false'}:
            raise ValueError('CH_SECURE должен быть true/false')
        if secure == 'true' and int(os.environ['CH_PORT']) == 8123:
            raise ValueError('CH_PORT=8123 требует CH_SECURE=false')
        client = clickhouse_connect.get_client(
            host=os.environ['CH_HOST'], port=int(os.environ['CH_PORT']),
            username=os.environ['CH_USER'], password=os.environ['CH_PASSWORD'],
            secure=secure == 'true', database='CDM')
        try:
            frame = client.query_df(Path(__file__).with_name('conversions_ch.sql').read_text('utf-8'),
                                    parameters={'date_from': f'{start} 00:00:00',
                                                'date_until': f'{until} 00:00:00'})
        finally:
            client.close()
        frame.columns = frame.columns.str.lower()
        missing = frame['order_id'].isna() | frame['client_id'].isna()
        logger.info(f'CH: rows={len(frame)}; skipped_without_id={int(missing.sum())}; MSK={start}..{until} exclusive')
        frame = frame.loc[~missing].copy()
        frame['sale_date'] = pd.to_datetime(frame['sale_date']).dt.date
        frame['event_id'] = [make_event_id(r) for r in frame.itertuples()]
        if frame['event_id'].duplicated().any():
            raise ValueError('CH вернул повторяющиеся event_id')
        now_ts = int(datetime.now(MSK).timestamp())
        missing_price = frame['price'].isna()
        missing_datetime = frame['datetime'].isna()
        future_datetime = frame['datetime'] > now_ts
        if missing_price.any() or missing_datetime.any() or future_datetime.any():
            # Только агрегаты: не выводим ClientId, заказы и другие клиентские данные.
            null_price_by_target = frame.loc[missing_price].groupby('target').size().to_dict()
            max_future_seconds = (
                int(frame.loc[future_datetime, 'datetime'].max() - now_ts)
                if future_datetime.any() else 0
            )
            raise ValueError(
                f'Валидация конверсий: price_null={int(missing_price.sum())}; '
                f'price_null_by_target={null_price_by_target}; '
                f'datetime_null={int(missing_datetime.sum())}; '
                f'future_rows={int(future_datetime.sum())}; '
                f'max_future_seconds={max_future_seconds}; '
                f'now_msk={datetime.fromtimestamp(now_ts, MSK).isoformat()}'
            )
        # Атомарно заменяем очередь только этого запуска, сохраняя очереди остальных запусков.
        with conn.cursor() as cur:
            cur.execute(f'DELETE FROM {TABLE} WHERE run_id = %s', (run_id,))
            with cur.copy(f"COPY {TABLE} (run_id, {', '.join(COLUMNS)}) FROM STDIN") as copy:
                for row in frame[COLUMNS].itertuples(index=False, name=None):
                    copy.write_row((run_id,) + tuple(None if pd.isna(v) else v for v in row))
        conn.commit()
        logger.info(f'GP: run_id={run_id}; published={len(frame)}')


def make_batch_id(run_id, offset):
    """Уникальный ID попытки: offset может повторяться после исключения успешных строк."""
    return f'{run_id}-{uuid4().hex}-{offset}'


def send_conversions(run_id, ym_send_enabled='false', batch_rows=10000, *, local=False,
                     preview=False):
    enabled = str(ym_send_enabled).strip().lower()
    if enabled not in {'true', 'false'} or int(batch_rows) <= 0:
        raise ValueError('ym_send_enabled=true/false; batch_rows > 0')
    with connect_gp(local=local) as conn:
        ensure_tables(conn)
        with conn.cursor() as cur:
            # Выполняем отправки последовательно, включая ручные запуски вне расписания CronWorkflow.
            cur.execute("SELECT pg_advisory_lock(72706101)")
            cur.execute(Path(__file__).with_name('pending_conversions.sql').read_text('utf-8'), (run_id,))
            candidates = cur.fetchall()
            rows = [r[:6] for r in candidates if not r[6] and not r[7]]
            pending = sum(bool(r[8]) for r in candidates)
            previous_key_excluded = sum(not r[6] and bool(r[7]) for r in candidates)
        logger.info(f'Дедупликация по прежнему ключу: excluded={previous_key_excluded}')
        logger.info(f'GP queue: run_id={run_id}; new={len(rows)}; unresolved={pending}')
        if pending:
            raise RuntimeError('Есть PENDING: сверить партию в Метрике до повторной отправки')
        if preview:
            logger.info('Проверка завершена, отправка не выполнялась')
            return
        if enabled == 'false':
            raise RuntimeError(f'Отправка выключена: проверена очередь GP, rows={len(rows)}; ym_send_enabled=false')
        if not rows:
            return
        if not local:
            from dwh_libs.workflows.shared_scripts import set_team_envs
            set_team_envs('YM_sender')
        require_env(['ymCounter', 'ymToken'])
        counter, token = os.environ['ymCounter'], os.environ['ymToken']
        for offset in range(0, len(rows), int(batch_rows)):
            batch = rows[offset:offset + int(batch_rows)]
            # Очередь после retry короче: один offset не идентифицирует прежнюю партию.
            batch_id = make_batch_id(run_id, offset)
            frame = pd.DataFrame(batch, columns=COLUMNS[:6]).rename(columns=RENAME)
            # Записываем попытку ДО HTTP-запроса, чтобы не повторить прерванную отправку без сверки.
            with conn.cursor() as cur:
                cur.executemany(f'''INSERT INTO {LOG}
                    (event_id,client_id,target,datetime,price,status,batch_id)
                    VALUES (%s,%s,%s,%s,%s,'PENDING',%s)''',
                    [tuple(r[:5]) + (batch_id,) for r in batch])
            conn.commit()
            response = requests.post(
                f'https://api-metrika.yandex.net/management/v1/counter/{counter}/offline_conversions/upload',
                headers={'Authorization': f'OAuth {token}'}, params={'comment': batch_id},
                files={'file': ('conversions.csv', frame[UPLOAD_COLUMNS].to_csv(index=False).encode('utf-8'), 'text/csv')},
                timeout=60)
            if not response.ok:
                details = metrica_error_details(response, token)
                raise RuntimeError(
                    f'Метрика HTTP {response.status_code}; batch={batch_id} оставлен PENDING; {details}')
            upload = response.json().get('uploading', {})
            upload_id = upload.get('id')
            with conn.cursor() as cur:
                cur.execute(f'UPDATE {LOG} SET upload_id=%s WHERE batch_id=%s', (upload_id, batch_id))
            conn.commit()
            if (not upload_id or upload.get('source_quantity') != len(batch)
                    or upload.get('line_quantity') != len(batch)):
                raise RuntimeError(f'Не все строки подтверждены; batch={batch_id}; upload_id={upload_id}; сверить PENDING')
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {LOG} SET status='ACCEPTED' WHERE batch_id=%s", (batch_id,))
            conn.commit()
            logger.info(f'Metrica accepted: batch={batch_id}; upload_id={upload_id}; rows={len(batch)}; status={upload.get("status")}')


def main():
    parser = argparse.ArgumentParser(description='Локальный запуск CH -> GP -> Метрика без Vault')
    parser.add_argument('command', choices=['load', 'preview', 'send'])
    parser.add_argument('--run-id', required=True, help='Один идентификатор для загрузки, проверки и отправки')
    parser.add_argument('--date-from', default='', help='YYYY-MM-DD, включительно; пусто = вчера МСК')
    parser.add_argument('--date-to', default='', help='YYYY-MM-DD, включительно; пусто = сегодня МСК')
    parser.add_argument('--batch-rows', type=int, default=10000)
    parser.add_argument('--env-file', type=Path, help='Загрузить указанный .env; текущие переменные имеют приоритет')
    parser.add_argument('--send-enabled', action='store_true', help='Обязательный флаг для команды send')
    args = parser.parse_args()
    if not args.run_id.strip() or len(args.run_id) > 200:
        parser.error('--run-id должен содержать от 1 до 200 символов')
    if args.command == 'send' and not args.send_enabled:
        parser.error('Для отправки нужен --send-enabled; для проверки используйте preview')
    if args.command != 'send' and args.send_enabled:
        parser.error('--send-enabled допустим только для send')
    if args.command != 'load' and (args.date_from or args.date_to):
        parser.error('Период задаётся только для load; preview/send читают готовый срез')
    if args.env_file:
        from dotenv import load_dotenv
        if not args.env_file.is_file():
            parser.error('Указанный --env-file не найден')
        load_dotenv(args.env_file, override=False)
    if args.command == 'load':
        load_conversions(args.run_id, args.date_from, args.date_to, local=True)
    else:
        send_conversions(args.run_id, 'true' if args.send_enabled else 'false',
                         args.batch_rows, local=True, preview=args.command == 'preview')


if __name__ == '__main__':
    main()