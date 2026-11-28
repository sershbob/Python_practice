# def load_data(
#     table,
#     limit = 10000,
#     *,
#     local = False,
#     preview = True
# ):
#     print(table)
#     print(limit)
#     print(local)
#     print(preview)


# def validate_params(run_id:str, batch_rows:int):
#     run_id = run_id.strip()

#     if run_id == '':
#         raise ValueError('Некорректный run_id')

#     if batch_rows <= 0 or batch_rows > 10000:
#         raise ValueError('Значение batch_rows вне допустимого диапазона')
#     print('Успешная валидация')
    
#     return run_id, batch_rows


# validate_params('test_001', 500)  # ('test_001', 500)
# validate_params('   ', 500)       # ValueError
# validate_params('test_001', 0)    # ValueError
# validate_params('test_001', 15000) # ValueError

import os 


def get_batch_size() -> int:
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))

    if batch_size <= 0:
        raise ValueError('Значение вне допустимого диапазона')

    return batch_size