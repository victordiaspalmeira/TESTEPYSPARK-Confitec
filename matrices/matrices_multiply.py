import random
import time

from dotenv import dotenv_values

config = dotenv_values()


def generate_random_matrix(matrix_size):
    return [
        [random.randint(0, 9) for j in range(matrix_size)]
        for i in range(matrix_size)
    ]


def multiply_matrices_basic(matrix_A, matrix_B):
    assert len(matrix_A[0]) == len(matrix_B)
    matrix_size = len(matrix_A[0])

    result = [
        [0 for j in range(matrix_size)]
        for i in range(matrix_size)
    ]

    for i in range(matrix_size):
        for j in range(matrix_size):
            for k in range(matrix_size):
                result[i][j] += matrix_A[i][k] * matrix_B[k][j]

    return result


def multiply_matrices_transpose(matrix_A, matrix_B):
    assert len(matrix_A[0]) == len(matrix_B)
    matrix_size = len(matrix_A[0])

    matrix_B_T = list(zip(*matrix_B))
    return [
        [
            sum(matrix_A[i][k]*matrix_B_T[j][k] for k in range(matrix_size))
            for j in range(matrix_size)
        ]
        for i in range(matrix_size)
    ]


def print_matrix(matrix_size, matrix):
    for i in range(matrix_size):
        row = ""

        for j in range(matrix_size):
            row += str(matrix[i][j]) + "\t"
        print(f'[{row}]')
    print(f'{matrix_size}x{matrix_size}\n')


if __name__ == "__main__":
    # Set matrix size and create the two base matrices.
    matrix_size = int(config['MATRIX_SIZE'])

    matrix_A = generate_random_matrix(matrix_size)
    print('Matrix A:')
    print_matrix(matrix_size, matrix_A)

    matrix_B = generate_random_matrix(matrix_size)
    print('Matrix B:')
    print_matrix(matrix_size, matrix_B)

    # Performs basic calculation.
    start_time_b = time.process_time_ns()
    matrix_C_basic = multiply_matrices_basic(matrix_A, matrix_B)
    end_time_b = time.process_time_ns()
    print('Matrix C - Basic Calculation:')
    print_matrix(matrix_size, matrix_C_basic)
    print(
        f'Elapsed time - Basic Calc: {end_time_b - start_time_b} ns\n'
    )

    # Perfoms transpose calculation.
    start_time_t = time.process_time_ns()
    matrix_C_t = multiply_matrices_transpose(matrix_A, matrix_B)
    end_time_t = time.process_time_ns()
    print('Matrix C - Transpose Calculation:')
    print_matrix(matrix_size, matrix_C_t)
    print(
        f'Elapsed time - Transpose Calc: {end_time_t - start_time_t} ns\n'
    )
