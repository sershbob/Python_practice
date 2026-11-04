# Напиши функцию-генератор even_numbers(n), которая выдаёт все чётные числа от 0 до n включительно.

def even_numbers(n):
    for n in range(0, n+1, 2):
        yield (n)

for number in even_numbers(10):
    print(number)

# Напиши генератор divisible_numbers(n), который выдаёт числа от 1 до n включительно, если они одновременно делятся на 3 и на 5 без остатка.

def divisible_numbers(n):
    for i in range(1, n+1):
        if i%3 == 0 and i%5 == 0:
            yield (i)

for number in divisible_numbers(50):
    print(number)

# Напиши генератор running_sum(numbers), который последовательно возвращает накопленную сумму элементов списка.