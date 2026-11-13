# Задание: напиши генератор filter_sales(sales, min_revenue), 
# который выдаёт только те словари, у которых значение revenue больше или равно min_revenue.

sales = [
    {'partner': 'АльфаСтрахование', 'revenue': 1500},
    {'partner': 'Ингосстрах', 'revenue': 2300},
    {'partner': 'Согласие', 'revenue': 800},
    {'partner': 'АльфаСтрахование', 'revenue': 3200},
    {'partner': 'РЕСО', 'revenue': 1200}
]

# пример использования 

for sale in filter_sales(sales, 2000):
    print(sale)