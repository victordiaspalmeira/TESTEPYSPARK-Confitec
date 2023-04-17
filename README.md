# Avaliação Confitec

Este repositório contém os dois programas solicitados para os desafios apresentados durante o processo seletivo da Confitec.

# Pyspark - Netflix Originals ETL

Extract, transform and load data from NetflixOriginals database. Implemented in PySpark 3.1.1.

The `.env` file defines some important environmental variables:
- 1. AWS Credentials
- 2. Spark configuration options
- 3. Input file path and output bucket/directory

The following transformations were applied sequentially to the raw data:
- 1. Remove duplicate rows present in raw data.
- 2. Transform 'Premiere' and 'dt_inclusao' fields from string to date type.
- 3. Transform all 'TBA' in field 'Seasons' to 'a ser anunciado'.
- 4. Add new field 'Data de Alteração' containing current timestamp
- 5. Order transformed data by 'Active' and 'Genre' fields.
- 6. Translate all fields to pt-br language.
- 7. Select the following fields: 'Titulo' 'Gênero', 'Temporada', 'Estreia', 'Idioma', 'Ativo', 'Estado', 'dt_inclusao' and 'Data de Alteração'.

This script can monitor Spark active jobs, raising a exception in case of one or more failing. Output data is uploaded to an AWS S3 Bucket.

# Matrix Multiplication

Implemented two funcions. Both functions performs matrix multiplication between two squared matrices `matrix_A` and `matrix_B`. Also, implemented an auxiliar function to print visually acceptable matrices.

 ## `multiply_matrices_basic` 
 
 The function first checks that the number of columns in matrix_A is equal to the number of rows in matrix_B, which is a necessary condition for matrix multiplication. The assertion check has a constant time complexity of O(1). 
 
 The function then creates a new matrix result with dimensions equal to the number of rows and columns of the input matrices. The creation of the result matrix has a time complexity of O(matrix_size^2), where matrix_size is the number of rows (or columns) of the input matrices. 
 
 Next, the function uses nested loops to iterate over the rows and columns of the result matrix. For each element in the result matrix, the function calculates the dot product of the corresponding row in matrix_A and column in matrix_B, which involves a third nested loop that iterates over the columns of matrix_A and the rows of matrix_B. The number of operations required to calculate all the elements of the result matrix is O(matrix_size^3).
 
 Therefore, the overall time complexity of the multiply_matrices_basic function is O(matrix_size^3), where matrix_size is the number of rows (or columns) of the input matrices.

## `multiply_matrices_transpose` 

The function first checks that the number of columns in matrix_A is equal to the number of rows in matrix_B, which is a necessary condition for matrix multiplication. The assertion check has a constant time complexity of O(1).

The function then creates a new matrix matrix_B_T that is the transpose of matrix_B. Transposing a matrix involves swapping its rows and columns. The creation of matrix_B_T has a time complexity of O(matrix_size^2), where matrix_size is the number of rows (or columns) of the input matrices.

Next, the function uses nested loops to iterate over the rows and columns of the result matrix, similar to the multiply_matrices_basic function. However, instead of iterating over the columns of matrix_B, the function uses the transposed matrix matrix_B_T, which allows for more efficient memory access during the calculation of the dot product.

For each element in the result matrix, the function calculates the dot product of the corresponding row in matrix_A and column in matrix_B_T, which involves a third nested loop that iterates over the columns of matrix_A and the rows of matrix_B_T. The number of operations required to calculate all the elements of the result matrix is O(matrix_size^3).

The overall time complexity of the multiply_matrices_transpose function is also O(matrix_size^3), where matrix_size is the number of rows (or columns) of the input matrices. However, the use of the transposed matrix matrix_B_T can lead to more efficient memory access during the calculation of the dot product, resulting in better performance compared to the multiply_matrices_basic function in some cases.
