"""Срез Юкассы: витрины, платежи и агрегаты GP -> Google Sheets."""
import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
import pandas as pd
import psycopg
import requests

from mapping import AFFILIATES, BLOCKS

try:
    from dwh_libs.dwh_logger import dwh_logger as logger
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent / 'sql'
TABLE = 'team_osago.gs_update_yookassa'
RUNS = 'team_osago.gs_update_yookassa_run'
DATA_COLUMNS = ['day', 'week', 'month', 'partner_name', 'orders', 'purch']
BLOCK_CODES = [b['block_code'] for b in BLOCKS]


def connect_gp(local=False):
    if not local:
        from dwh_libs.workflows.shared_scripts import set_dwh_envs
        from vault_envs import load_connection_envs
        set_dwh_envs()
        load_connection_envs()
    return psycopg.connect(host=os.environ['GP_HOST'], port=os.environ['GP_PORT'],
        dbname=os.environ['GP_DATABASE'], user=os.environ['GP_USER'],
        password=os.environ['GP_PASSWORD'])


def resolve_period(date_from='', date_to='', weeks=2, today=None):
    today = today or datetime.now().date()
    monday = today - timedelta(days=today.weekday())
    if int(weeks) < 1:
        raise ValueError('weeks должен быть положительным')
    start = date.fromisoformat(date_from) if date_from else monday - timedelta(weeks=int(weeks))
    end = date.fromisoformat(date_to) if date_to else monday
    if start >= end or end > monday or start.weekday() or end.weekday():
        raise ValueError('Границы — понедельники: date_from < date_to <= текущий понедельник')
    return start, end


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
            run_id text NOT NULL, block_code text NOT NULL, day date NOT NULL,
            week date NOT NULL, month date NOT NULL, partner_name text NOT NULL,
            orders bigint NOT NULL, purch bigint NOT NULL,
            loaded_at timestamptz NOT NULL DEFAULT current_timestamp
        ) DISTRIBUTED BY (block_code)''')
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {RUNS} (
            run_id text NOT NULL, date_from date NOT NULL, date_to date NOT NULL,
            block_code text NOT NULL, source_ready boolean NOT NULL, source_last_dt date,
            loaded_at timestamptz NOT NULL DEFAULT current_timestamp
        ) DISTRIBUTED BY (run_id)''')
    conn.commit()


def normalize_days(frame):
    frame.columns = frame.columns.str.lower()
    frame['day'] = pd.to_datetime(frame['day']).dt.date
    return frame


def build_blocks(orders, web, app, ready):
    """Не размножаем заказы при соединении с событиями одного дня."""
    parts = []
    for prefix, mask in [('web', orders.interface.eq('Web')),
                         ('app', orders.interface.str.startswith('App', na=False)),
                         ('api', orders.interface.eq('API'))]:
        segment = orders.loc[mask].copy()
        segment['block_code'] = prefix + '_segment_sale'
        parts.append(segment)
        code = prefix + '_to_sale'
        if prefix != 'api' and ready[code]:
            key, events = ('client_id', web) if prefix == 'web' else ('user_id', app)
            # pandas соединяет NULL с NULL, поэтому исключаем их явно, как SQL JOIN.
            keys = events.loc[events[key].notna(), [key, 'day']].drop_duplicates()
            matched = segment.loc[segment[key].notna()].merge(keys, on=[key, 'day'], how='inner', validate='many_to_one')
            matched['block_code'] = code
            parts.append(matched)
    return pd.concat(parts, ignore_index=True)[['block_code', 'order_id', 'day', 'partner_name', 'is_purchase']]


def load_snapshot(run_id, date_from='', date_to='', weeks=2, local=False):
    with connect_gp(local) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT current_date')
            today = cur.fetchone()[0]
        start, end = resolve_period(date_from, date_to, weeks, today=today)
        ensure_tables(conn)
        params = {'date_from': start, 'date_to': end}
        latest = {}
        sources = [('web_to_sale', 'mart.osago_funnel', 'event_dt'),
                   ('app_to_sale', 'mart.osago_mobile_app', 'event_dt'),
                   ('orders', 'mart.calculation_osago', 'calculation_datetime')]
        with conn.cursor() as cur:
            missing = []
            for _, source, _ in sources:
                cur.execute('SELECT to_regclass(%s)', (source,))
                if cur.fetchone()[0] is None:
                    missing.append(source)
            cur.execute('SELECT to_regclass(%s)', ('stg2.osago__osagoorderpayment',))
            if cur.fetchone()[0] is None:
                missing.append('stg2.osago__osagoorderpayment')
            if missing:
                raise RuntimeError('В GP отсутствуют источники: ' + ', '.join(missing))
            for code, source, field in sources:
                cur.execute(f'SELECT max({field})::date FROM {source}')
                latest[code] = cur.fetchone()[0]
        if latest['orders'] is None or latest['orders'] < end - timedelta(days=1):
            raise RuntimeError('Витрина заказов не покрывает конец периода; публикация отменена')
        ready = {code: not code.endswith('_to_sale') or
                 (latest[code] is not None and latest[code] >= end - timedelta(days=1))
                 for code in BLOCK_CODES}

        def query(name):
            with conn.cursor() as cur:
                cur.execute((SQL_DIR / name).read_text('utf-8'), params)
                return normalize_days(pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description]))

        orders = query('orders_gp.sql')
        if orders.empty:
            raise RuntimeError('GP не вернул заказы; прежний срез сохранён')
        if orders[['order_id', 'day', 'partner_name', 'is_purchase']].isna().any().any():
            raise ValueError('Обязательные поля заказов содержат NULL')
        if not orders.is_purchase.isin([0, 1]).all():
            raise ValueError('Некорректный признак покупки')
        web = query('web_payment_gp.sql') if ready['web_to_sale'] else None
        app = query('app_payment_gp.sql') if ready['app_to_sale'] else None
        frame = build_blocks(orders, web, app, ready)
        with conn.cursor() as cur:
            # Все изменения среза фиксируются вместе; ошибка сохраняет прежние данные.
            cur.execute(f'LOCK TABLE {RUNS} IN EXCLUSIVE MODE')
            cur.execute('''CREATE TEMP TABLE tmp_yookassa_orders (
                block_code text, order_id text, day date, partner_name text, is_purchase integer
            ) ON COMMIT DROP DISTRIBUTED BY (order_id)''')
            with cur.copy('COPY tmp_yookassa_orders (block_code,order_id,day,partner_name,is_purchase) FROM STDIN') as copy:
                for row in frame.itertuples(index=False, name=None):
                    copy.write_row(row)
            cur.execute(f'DELETE FROM {TABLE} WHERE run_id=%s', (run_id,))
            cur.execute(f'DELETE FROM {RUNS} WHERE run_id=%s', (run_id,))
            for statement in (SQL_DIR / 'publish_gp.sql').read_text('utf-8').split(';'):
                if statement.strip():
                    cur.execute(statement, {'run_id': run_id})
            cur.executemany(f'''INSERT INTO {RUNS}
                (run_id,date_from,date_to,block_code,source_ready,source_last_dt)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                [(run_id,start,end,b,ready[b],latest.get(b,latest['orders'])) for b in BLOCK_CODES])
        conn.commit()
    logger.info(f'Срез опубликован: run_id={run_id}; период=[{start}, {end}); готовность={ready}')


def read_snapshot(run_id, local=False):
    with connect_gp(local) as conn:
        with conn.cursor() as cur:
            # Согласованно читаем метаданные и строки, пока загрузчик не меняет срез.
            cur.execute(f'LOCK TABLE {RUNS} IN SHARE MODE')
            cur.execute(f'SELECT block_code,source_ready,date_from,date_to,source_last_dt FROM {RUNS} WHERE run_id=%s', (run_id,))
            metadata = cur.fetchall()
            if len(metadata) != len(BLOCK_CODES) or {r[0] for r in metadata} != set(BLOCK_CODES):
                raise RuntimeError('Не найден полный опубликованный срез для run_id')
            cur.execute(f'''SELECT block_code,day,week,month,partner_name,orders,purch
                FROM {TABLE} WHERE run_id=%s ORDER BY day,partner_name''', (run_id,))
            frame = pd.DataFrame(cur.fetchall(), columns=['block_code'] + DATA_COLUMNS)
    if len({(r[2], r[3]) for r in metadata}) != 1:
        raise RuntimeError('Границы периода различаются между блоками среза')
    if not set(frame.block_code).issubset(BLOCK_CODES):
        raise RuntimeError('В срезе найден неизвестный блок')
    if frame.duplicated(['block_code', 'day', 'partner_name']).any():
        raise RuntimeError('В срезе найдены повторяющиеся строки агрегатов')
    if ((frame.purch < 0) | (frame.orders < frame.purch)).any():
        raise RuntimeError('Некорректные счётчики заказов и покупок')
    return frame, {r[0]: r[1:] for r in metadata}


def retry_google(func):
    for attempt in range(5):
        try:
            return func()
        except gspread.exceptions.APIError as exc:
            if exc.response.status_code != 429 and exc.response.status_code < 500:
                raise
            if attempt == 4:
                raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError):
            if attempt == 4:
                raise
        time.sleep(min(30, 2 ** attempt))


def prepare_range(values, records, start_column, end_column, first_row, start, end, date_values=None):
    """Заменяем только окно, сохраняя строки до и после него; хвост очищаем той же записью."""
    dates = []
    for index, row in enumerate(values[first_row - 1:], start=first_row - 1):
        raw = row[0] if row else ''
        if isinstance(raw, str) and raw.startswith('=') and date_values is not None:
            raw = date_values[index][0] if index < len(date_values) and date_values[index] else ''
        if not str(raw).strip() and any(str(cell).strip() for cell in row):
            raise ValueError('Найдена непустая строка без даты; обновление отменено')
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            # Google Sheets хранит даты как число дней от 30 декабря 1899 года.
            parsed = pd.to_datetime(raw, unit='D', origin='1899-12-30', errors='coerce')
        elif isinstance(raw, str) and '.' in raw:
            parsed = pd.to_datetime(raw, format='%d.%m.%Y', errors='coerce')
        else:
            parsed = pd.to_datetime(str(raw), errors='coerce')
        if str(raw).strip() and pd.isna(parsed):
            raise ValueError(f'Нераспознанная дата в {start_column}{index + 1}: {str(raw)[:80]!r}')
        dates.append(parsed.date() if pd.notna(parsed) else None)
    nonempty = [d for d in dates if d is not None]
    if nonempty != sorted(nonempty):
        # Исторические выгрузки могли добавить периоды не по порядку.
        # Перестраиваем только шесть колонок блока, сохраняя строки вне окна.
        preserved = []
        for existing, day in zip(values[first_row - 1:], dates):
            if day is None:
                if any(str(cell).strip() for cell in existing):
                    raise ValueError('Найдена непустая строка без даты; автоматическая сортировка отменена')
                continue
            if not start <= day < end:
                row = list(existing[:6]) + [''] * max(0, 6 - len(existing))
                preserved.append((day, row))
        combined = preserved + [(date.fromisoformat(row[0]), row) for row in records]
        combined.sort(key=lambda item: item[0])
        content = [row for _, row in combined]
        height = max(len(dates), len(content))
        content += [[''] * 6 for _ in range(height - len(content))]
        return f'{start_column}{first_row}:{end_column}{first_row + height - 1}', content
    lower = next((i for i, d in enumerate(dates) if d is not None and d >= start), len(dates))
    upper = next((i for i, d in enumerate(dates) if d is not None and d >= end), len(dates))
    width = 6
    suffix = [list(r[:width]) + [''] * max(0, width - len(r)) for r in values[first_row - 1 + upper:]]
    content = records + suffix
    old_height = len(dates) - lower
    height = max(old_height, len(content))
    if height == 0:
        return None
    content += [[''] * width for _ in range(height - len(content))]
    row = first_row + lower
    return f'{start_column}{row}:{end_column}{row + height - 1}', content


def sheet_input_values(rows):
    """Сохраняем формулы, а обычные строки передаём как текст без автопреобразования дат."""
    return [["'" + value if isinstance(value, str) and value and not value.startswith('=')
             else value for value in row] for row in rows]


def ensure_sheet_sizes(updates):
    """Расширяем каждый лист один раз до максимума всех его блоков, никогда не уменьшаем."""
    sheets = {}
    for worksheet, _, target, _, _ in updates:
        key = (worksheet.spreadsheet_id, worksheet.id)
        required = int(''.join(c for c in target.split(':')[1] if c.isdigit()))
        if key not in sheets:
            sheets[key] = [worksheet, worksheet.row_count]
        sheets[key][1] = max(sheets[key][1], worksheet.row_count, required)
    for worksheet, required in sheets.values():
        if required > worksheet.row_count:
            retry_google(lambda: worksheet.resize(rows=required))


def publish_snapshot(run_id, send_enabled='false', local=False, preview=False):
    enabled = str(send_enabled).lower()
    if enabled not in {'true', 'false'}:
        raise ValueError('send_enabled должен быть true/false')
    frame, metadata = read_snapshot(run_id, local)
    for code in BLOCK_CODES:
        part = frame[frame.block_code == code]
        logger.info(f'{code}: ready={metadata[code][0]}; rows={len(part)}; orders={part.orders.sum()}; purch={part.purch.sum()}; last={metadata[code][3]}')
    if preview:
        return
    if enabled != 'true':
        raise RuntimeError('Отправка в Google Sheets выключена; срез GP подготовлен')
    if not local:
        from dwh_libs.workflows.shared_scripts import set_team_envs
        set_team_envs('gs_update')
    gs = gspread.service_account_from_dict(json.loads(os.environ['gs_key']))
    updates = []
    for affiliate in AFFILIATES:
        sheet = retry_google(lambda: gs.open_by_key(os.environ[affiliate['spreadsheet_key_env']]))
        for block in affiliate['blocks']:
            code = block['block_code']
            ready, start, end, last = metadata[code]
            if not ready:
                logger.warning(f'Пропущен {code}: источник заканчивается {last}; данные Sheets сохранены')
                continue
            part = frame.loc[frame.block_code == code, DATA_COLUMNS].copy()
            for column in ['day', 'week', 'month']:
                part[column] = part[column].map(lambda v: v.isoformat())
            records = part.values.tolist()
            worksheet = retry_google(lambda: sheet.worksheet(block.get('data_sheet', affiliate['data_sheet'])))
            left, right = block['metric_data_range_start'], block['metric_data_range_end']
            # Формулы вне окна сохраняем как формулы, а не как рассчитанные значения.
            values = retry_google(lambda: worksheet.get(f'{left}:{right}', value_render_option='FORMULA'))
            date_values = None
            if any(row and isinstance(row[0], str) and row[0].startswith('=') for row in values):
                date_values = retry_google(lambda: worksheet.get(f'{left}:{left}', value_render_option='FORMATTED_VALUE'))
            update = prepare_range(values, records, left, right, affiliate.get('data_start_row', 2),
                                   start, end, date_values=date_values)
            if update:
                target, content = update
                updates.append((worksheet, code, target, content, len(records)))
    # Проверяем все диапазоны до первой записи, чтобы ошибки структуры листа
    # не приводили к частичному обновлению предыдущих блоков.
    ensure_sheet_sizes(updates)
    for worksheet, code, target, content, row_count in updates:
        retry_google(lambda: worksheet.update(range_name=target, values=sheet_input_values(content),
                                              value_input_option='USER_ENTERED'))
        logger.info(f'Обновлён {code}: {target}; rows={row_count}')


def notify_error(error):
    """Сохраняем существующий алерт в Time для запусков Argo."""
    try:
        from dwh_libs.workflows.shared_scripts import set_team_envs
        from dwh_libs.connectors.time_connector import TimeClient
        set_team_envs('time_bots_info')
        TimeClient(time_token=os.environ['time_bot_token']).send_time_main_message(
            os.environ['osago-pa'], f'gs_update_yookassa: ошибка {type(error).__name__}; проверьте логи Argo')
    except Exception:
        logger.error('Не удалось отправить алерт в Time')


def main():
    parser = argparse.ArgumentParser(description='Локальная загрузка Юкассы без Vault')
    parser.add_argument('command', choices=['load', 'preview', 'send'])
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--date-from', default='')
    parser.add_argument('--date-to', default='', help='Исключающая граница, понедельник')
    parser.add_argument('--weeks', type=int, default=2)
    parser.add_argument('--env-file', type=Path)
    parser.add_argument('--send-enabled', action='store_true')
    args = parser.parse_args()
    if not args.run_id.strip():
        parser.error('run-id не должен быть пустым')
    if args.command == 'send' and not args.send_enabled:
        parser.error('Для send нужен --send-enabled')
    if args.command != 'send' and args.send_enabled:
        parser.error('--send-enabled допустим только для send')
    if args.command != 'load' and (args.date_from or args.date_to or args.weeks != 2):
        parser.error('Период задаётся только для load')
    if args.env_file:
        from dotenv import load_dotenv
        if not args.env_file.is_file():
            parser.error('env-file не найден')
        load_dotenv(args.env_file, override=False)
    if args.command == 'load':
        load_snapshot(args.run_id, args.date_from, args.date_to, args.weeks, local=True)
    else:
        publish_snapshot(args.run_id, 'true' if args.send_enabled else 'false', local=True, preview=args.command == 'preview')


if __name__ == '__main__':
    main()
