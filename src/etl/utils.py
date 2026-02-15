import random
import string

import polars as pl


def id_generator(size=6, chars=string.ascii_uppercase):
    """Generate a random string of specified length using given characters.

    Args:
        size: Length of the generated string. Defaults to 6.
        chars: String of characters to choose from. Defaults to uppercase
            ASCII letters.

    Returns:
        A random string of the specified size.
    """
    return "".join(random.choice(chars) for _ in range(size))


def clean_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Return a copy of ``df`` where all Boolean columns are converted to numeric
    (Int8) columns with 1 for True and 0 for False.
    """

    list_cols = [
        col_name
        for col_name, dtype in zip(df.columns, df.dtypes)
        if isinstance(dtype, pl.List) or isinstance(dtype, pl.Struct)
    ]
    df = df.drop(list_cols)
    # Identify Boolean columns
    bool_cols = [c for c, t in df.schema.items() if t == pl.Boolean]

    # If there are none, just return the original frame
    if not bool_cols:
        return df

    # Convert each Boolean column to Int8 (1/0)
    # ``cast(pl.Int8)`` works because Polars treats True/False as 1/0 internally.
    conversions = [pl.col(col).cast(pl.Int8).alias(col) for col in bool_cols]

    # Apply the conversions and return a new DataFrame
    return df.with_columns(conversions)
