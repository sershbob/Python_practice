# Напиши функцию-генератор even_numbers(n), которая выдаёт все чётные числа от 0 до n включительно.

def even_numbers(n):
    for n in range (0, n+1, 2):
        yield (n)

for number in even_numbers(567):
    print(number)