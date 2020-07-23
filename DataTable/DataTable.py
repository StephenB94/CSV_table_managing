import pandas as pd
from numpy import nan
from .utils import *
import os


class DataTable(object):
    """
    Load a CSV file and allows editing and managing it by dictionaries parameter.
    Methods:
        load() - reload data frame
        save(path) - export in csv
        to_csv_string - return data frame as string in csv format
        get(query) - return data frame extraction
        delete(query) - cancel data from data frame
        edit(query) - replace data in data frame
        append(query) - add data in data frame
    """

    def __init__(self, path_or_dict=None, sep=';', replace_nan=None):
        """
        Initialize data table
        Args:
            path_or_dict: <str> or <dict> path to file or dictionary representation
            sep: <str> separator symbol
            replace_nan: replace nan with the given value, if None there will be no replacement
        """
        if isinstance(path_or_dict, str):
            assert os.path.exists(path_or_dict), " <DataTable.__init__> Given path does not exist."
            self.__path = path_or_dict
            self.__load(replace_nan)
        elif isinstance(path_or_dict, dict):
            self.__load_from_dict(path_or_dict)
            self.__path = None
        else:
            self._df = pd.DataFrame()
        self.__sep = sep

        self.__keywords = {'columns', 'state', 'values'}

    def __load(self, replace_nan):
        self._df = pd.read_csv(self.__path, sep=self.__sep)
        if replace_nan is not None:
            self._df = self._df.replace(nan, '', regex=True)

    def __load_from_dict(self, table):
        """
        create a data table from a dict
        :param table: <dict>
        """
        self._df = pd.DataFrame(table)

    def save(self, path=None, sep=None):
        """
        Save changes on file or saves a new file in the given path
        Args:
            path: <str> path to file
            sep: <str> separator symbol
        Returns: None
        """
        path = self.__path if path is None else path
        sep = self.__sep if sep is None else sep

        self._df.to_csv(path, sep=sep, index=False)

    def to_string(self, sep=';'):
        """
        Returns data frame as a string in csv format
        Args:
            sep: <str> separator symbol
        Returns: string
        """
        sep = self.__sep if sep is None else sep
        text = self._df.to_csv(path_or_buf=None, sep=sep, index=False)

        text = text.replace("\"", "").replace("\r\n", "\n")
        if text[-1] == '\n':
            text = text[:-1]
        return text

    def get(self, query=None, dict_type='list'):
        """
        Reads data from the data table
        Args:
            query: dictionary with the following structure
                    {'columns': {column1, ..., columnX},
                     'state': {column1: value1, ..., columnX: valueX}}
            dict_type: Determines the type of the values of the dictionary.
                            - ‘dict’ (default) : dict like {column -> {index -> value}}
                            - ‘list’ : dict like {column -> [values]}
                            - ‘series’ : dict like {column -> Series(values)}
                            - ‘split’ : dict like {‘index’ -> [index], ‘columns’ -> [columns], ‘data’ -> [values]}
                            - ‘records’ : list like [{column -> value}, … , {column -> value}]
                            - ‘index’ : dict like {index -> {column -> value}}

        Returns: {column1: [value1_1, ..., value1_Y],
                  ...,
                  columnX: [valueX_1, ..., valueX_Y]}
        Examples:
            query = {'state': {'animal': 'dog', 'color': 'red'},
                     'columns': {'animal'}}
        """
        if query is None:
            query = dict()
        if 'state' not in query:
            query['state'] = {}

        self.__query_validation(query, must_have={'state'}, source=self.get.__name__)

        table = self.__filter(query['state'])

        if 'columns' in query:
            table = table[query['columns']]
            if isinstance(table, pd.Series):
                table = table.to_frame()
        return table.to_dict(dict_type)

    def delete(self, query, save=True):
        """
        Delete data from data frame
        :param query: {'state': {column1: value1, ..., columnX: valueX}}
        :param save: True to overwrite the csv files
        """
        if not isinstance(save, bool):
            raise TypeError(f"<DataTable.Delete> Expected bool, given: {type(save)}")

        self.__query_validation(query, must_have={'state'}, source=self.delete.__name__)

        self._df = self._df[(self._df[query['state']] != pd.Series(query['state'])).all(axis=1)]

        if save:
            self.save()

    def edit(self, query, save=True):
        """
        Replace values on data frame rows
        :param query: {'state': {column1: value1, ..., columnX: valueX}
                       'values': {column1: value1,
                                  ...
                                  columnX: valueX}}
        :param save: True to overwrite the csv files
        """
        if not isinstance(save, bool):
            raise TypeError(f"<DataTable.Delete> Expected bool, given: {type(save)}")

        self.__query_validation(query, must_have={'state', 'values'}, source=self.edit.__name__)

        state = query['state']
        for kw in query['values']:
            self._df.loc[(self._df[state] == pd.Series(state)).all(axis=1), kw] = query['values'][kw]

        if save:
            self.save()

    def append(self, query, save=True):
        """
        Add data to the data frame
        :param query: {'values': {column1: [value1_1, ..., value1_Y],
                                 ...
                                 columnX: [valueX_1, ..., valueX_Y]}} type list
                    or
                    {'values': [{column1: value1_1,
                                 ...
                                 columnX: valueX_1}, ...]} type records
        :param save: Set to False to not overwrite the file
        """
        if not isinstance(save, bool):
            raise TypeError(f"<DataTable.Delete> Expected bool, given: {type(save)}")

        self._df = self._df.append(pd.DataFrame(query['values']), ignore_index=True)

        if save:
            self.save()

    def max(self, query):
        """
        Get the maximum value from a column of numbers.
        If it is an empty column will be returned 0
        :param query: {'columns': {column1, ..., columnX},
                       'state': {column1: value1, ..., columnX: valueX}}
        :return: {column1: [value1_1, ..., value1_Y],
                  ...,
                  columnX: [valueX_1, ..., valueX_Y]}
        """
        if 'state' not in query:
            query['state'] = {}
        self.__query_validation(query, must_have={'columns'}, source=self.max.__name__)

        table = self.__filter(query['state'])
        if 'columns' in query:
            table = table[query['columns']]

        max_values = {}
        for col in table.columns:
            value = table[col].max()
            max_values[col] = value if value is not nan else 0

        return 0 if max_values is nan else max_values

    @property
    def columns(self):
        return self._df.columns

    def __filter(self, state):
        return self._df.loc[((self._df[state] == pd.Series(state)).all(axis=1))]

    def __query_validation(self, query, must_have=None, source=""):
        if not isinstance(query, dict):
            raise TypeError(f"<Base.FileSystem.{source}> Invalid type in input"
                            f"\n\t\tGot: {type(query)}, Expected: dict")

        if not are_contained(set(query), self.__keywords):
            raise ValueError(f"<Base.FileSystem.{source}> Invalid keywords"
                             f"\n\t\tGot: {set(query)}, Expected: {self.__keywords}")

        if must_have is not None:
            assertion(set(extract_match(set(query), set(must_have))) == set(must_have),
                      f"<DataTable.__query_validation> Query kws: {set(query)}"
                      f"\nMust have: {set(must_have)}")

        for kw in extract_match(set(query), self.__keywords):
            columns = query[kw] if kw == 'columns' else list(query[kw].keys())
            if not are_contained(set(columns), set(self._df.columns)):
                raise ValueError(f"<Base.FileSystem.{source}> Invalid keywords"
                                 f"\n\t\tGot: {columns}, Expected: {set(self._df.columns)}")

    @property
    def query_keys(self):
        return self.__keywords
