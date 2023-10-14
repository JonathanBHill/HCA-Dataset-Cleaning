import pandas as pd
import numpy as np
# import matlibplot.pyplot as plt


class Cleaning:
	def __init__(self, query, sql_engine):
		self.query = query
		self.engine = sql_engine
		self.df = self.populate_df(self.query)
		self.final_df = None
		self._correction = None
		self._cat_arr = None
		self._order = None
		self._save_cols = None


	@property
	def correction(self):
		return self._correction

	@correction.setter
	def correction(self, criteria: list):
		self._correction = criteria

	@property
	def cat_arr(self):
		return self._cat_arr

	@cat_arr.setter
	def cat_arr(self, cat_array: list):
		self._cat_arr = cat_array

	@property
	def order(self):
		return self._order

	@order.setter
	def order(self, cat_array: list):
		self._order = cat_array

	@property
	def save_cols(self):
		return self._save_cols

	@save_cols.setter
	def save_cols(self, col_array: list):
		self._save_cols = col_array

	def correct_columns(self, col, arguments=None):
		if len(arguments) == 2:
			old, new = arguments
			a = np.array(self.df[col].values.tolist())
			a = np.where(a == old, new, a).tolist()
		else:
			a = np.array(self.df[col].values.tolist())
			a = a.astype(int)
			a = np.where(a < -1, -1, a).tolist()

		self.df[col] = a
		self.df[col] = self.df[col].astype(int)
		if self.df.dtypes[-1] != "int64":
			self.df[self.df.columns[-1]] = \
				self.df[self.df.columns[-1]].str.split("\\r", n=1, expand=True)[0]

		return self.df

	def populate_df(self, query):
		connection = self.engine.connect()

		requested = pd.read_sql_query(query, connection)

		connection.detach()
		connection.close()

		return requested

	def _categorize_column(self, column, col_dict):
		col, vals = list(col_dict.items())[0]
		self.df[col] = self.df[column]
		self.df = self.df.replace(col_dict)

		return self.df

	def rearrange_columns(self, order):
		self.df = self.df[order]

		return self.df

	def categorize_columns(self, cat_array):
		arr_iter = iter(cat_array)
		for _ in cat_array:
			col, dictionary = next(arr_iter)
			self.df = self._categorize_column(col, dictionary)
		return self.df

	def drop_columns(self, cols_to_keep):
		self.final_df = self.df.iloc[:, cols_to_keep]

		return self.final_df

	def clean_df(self):
		assert self.correction, "self.correction has not been assigned any values."
		assert self.cat_arr, "self.cat_arr has not been assigned any values."
		assert self.order, "self.order has not been assigned any values."
		assert self.save_cols, "self.save_cols has not been assigned any values."

		if len(self.correction) > 1:
			self.correct_columns(self.correction[0], self.correction[1])
		else:
			self.correct_columns(self.correction[0])
		self.categorize_columns(self.cat_arr)
		self.rearrange_columns(self.order)
		self.drop_columns(self.save_cols)

		return self.final_df
