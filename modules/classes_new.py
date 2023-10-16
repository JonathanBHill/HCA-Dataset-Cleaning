# Imports
from pandas import DataFrame, Series, read_sql_query
from sqlalchemy import engine


def get_mysql_data(queries: list, sql_engine: engine) -> list:
    """
        Fetch data from a MySQL database using SQL queries.

        Args:
            queries (list): List of SQL queries to execute.
            sql_engine (engine): SQLAlchemy engine for database connection.

        Returns:
            list: A list of DataFrames, each containing the result of a query.
        """
    connection = sql_engine.connect()
    dfs = []
    if not isinstance(queries, list):
        queries = [queries]
    for index, query in enumerate(queries):
        dfs.append(read_sql_query(query, connection))
        print(f"Populated dataframe {index + 1} of {len(queries)}.")
    connection.detach()
    connection.close()
    
    return dfs


class Cleaning:
    """
    A class for cleaning and formatting Pandas DataFrames.

    Args:
        dataframe (DataFrame): Dataframe to be cleaned.
        dataframe_type (str, optional): Type of dataframe. Default is None.
        replacements (tuple, optional): Tuple specifying replacement
        criteria for data. Default is None.
        cat_column_names (dict, optional): Dictionary mapping
        categorical column names. Default is None.
        order (tuple, optional): Order of columns for dataframe. Default
        is None.
        columns_to_keep (tuple, optional): Tuple of columns to keep.
        Default is None.
    """
    def __init__(
            self, dataframe: DataFrame, dataframe_type: str = None,
            replacements: tuple = None, cat_column_names: dict = None,
            order: tuple = None, columns_to_keep: tuple = None
    ):
        """
        Initializes the Cleaning instance.

        Args:
        dataframe (DataFrame): Dataframe to be cleaned.
        dataframe_type (str, optional): Type of dataframe. Default is None.
        replacements (tuple, optional): Tuple specifying replacement
        criteria for data. Default is None.
        cat_column_names (dict, optional): Dictionary mapping
        categorical column names. Default is None.
        order (tuple, optional): Order of columns for dataframe. Default
        is None.
        columns_to_keep (tuple, optional): Tuple of columns to keep.
        Default is None.
        """
        self.df = dataframe
        self.df_type = dataframe_type
        self.replacement_values = replacements
        self.cat_col_names = cat_column_names
        self.column_order = list(order)
        self.keep_cols = list(columns_to_keep)
        self.final_df = self._clean_dataframe()
    
    def export_to_csv(self, filename: str = "cleaned.csv") -> None:
        """
        Exports the final cleaned dataframe as a CSV file.

        Args:
            filename (str, optional): Filename of the CSV file where the
            dataframe is saved to. Default is "cleaned.csv".
        """
        self.final_df.to_csv(filename)
        print(f"Dataframe has been saved as {filename}.")
    
    def _clean_dataframe(self) -> DataFrame:
        """
        Determines which cleaning method should be performed on dataframe
        based on what type of dataframe is given.

        Returns:
            DataFrame: The cleaned and formatted dataframe.
        """
        if self.df_type == "enc":
            return self._clean_enc_dataframe()
        elif self.df_type == "proc":
            return self._clean_proc_dataframe()
    
    def _clean_proc_dataframe(self) -> DataFrame:
        """
        Cleans and format a procedures dataframe.

        Returns:
            DataFrame: The cleaned and formatted procedures dataframe.
        """
        self._replace_invalid_values(self.replacement_values)
        self._categorical_preparation()
        self._rearrange_columns(self.column_order)
        final_df = self.drop_columns(self.keep_cols)
        return final_df
    
    def _clean_enc_dataframe(
            self, format_column: str = "Payer_Type"
    ) -> DataFrame:
        """
        Cleans and format an encounters dataframe.

        Returns:
            DataFrame: The cleaned and formatted encounters dataframe.
        """
        self._format_column(format_column)
        self._replace_invalid_values(self.replacement_values)
        self._categorical_preparation()
        self._rearrange_columns(self.column_order)
        final_df = self.drop_columns(self.keep_cols)
        return final_df
    
    def _format_column(self, col: str) -> None:
        """
        Formats a specific column in the dataframe.

        Args:
            col (str): The name of the column to format.

        Returns:
            None
        """
        if col == "Payer_Type":
            self.df[col] = self.df[col].apply(
                lambda x: x.rsplit(
                    "\r",
                    1
                )[0]
            )
    
    def _replace_invalid_values(
            self, replacement_criteria: tuple = None
    ) -> None:
        """
        Replaces values in a specific column of dataframe based on replacement
        criteria.

        Args:
            replacement_criteria (tuple, optional): Tuple specifying column
            and replacement values.

        Returns:
            None
        """
        col, replacements = replacement_criteria
        old, new = replacements
        if self.df_type == "enc":
            self.df[col] = self.df[col].apply(lambda x: new if x == old else x)
        elif self.df_type == "proc":
            self.df[col] = self.df[col].apply(lambda x: new if x <= old else x)
    
    def _categorical_preparation(self) -> None:
        """
        Replaces the values of specified columns in the cat_col_names dictionary
        to category-style integers.

        Returns:
            None
        """
        def get_categories(column: Series) -> dict:
            """
            Populates dictionary with unique values in the specified column
            which will be used to replace each value.

            Returns:
                dict: Dictionary with the unique values as keys and the
                replacement ID as the value.
            """
            categories = {}
            unique_values = self.df[column].unique()
            unique_values.sort()
            for ID, category in enumerate(unique_values):
                categories[category] = ID
            return categories
        
        for old_col, new_col in self.cat_col_names.items():
            category_instructions = get_categories(old_col)
            self.df[new_col] = self.df[old_col].replace(category_instructions)
    
    def drop_columns(self, cols_to_keep: list) -> DataFrame:
        """
        Drops columns based on indices specified in the cols_to_keep attribute.

        Args:
            cols_to_keep (list): Indices of columns to keep.

        Returns:
            DataFrame: The filtered dataframe containing only the specified
            columns.
        """
        return self.df.iloc[:, cols_to_keep]
    
    def _rearrange_columns(self, order: list) -> DataFrame:
        """
        Rearranges columns based on the specified order.

        Args:
            order (list): The desired order of columns.

        Returns:
            DataFrame: The dataframe with columns rearranged according to the
            order given.
        """
        self.df = self.df[order]
        return self.df
