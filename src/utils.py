from scipy.sparse import csr_matrix, coo_matrix


def build_csr_matrix(df, data_col, row_col, col_col):
    return csr_matrix(
        (df[data_col].to_numpy(),
         (df[row_col].to_numpy(),
          df[col_col].to_numpy())),
    )


def build_coo_matrix(df, data_col, row_col, col_col):
    return coo_matrix(
        (df[data_col].to_numpy(),
         (df[row_col].to_numpy(),
          df[col_col].to_numpy())),
    )