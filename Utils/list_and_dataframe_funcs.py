# type: ignore

# from icecream import ic

import pandas as pd


def NonNoneList(l: list) -> list:
    """
    Убирает все None из списка

    Args:
        l (list): исходный список

    Returns:
        list: список без None
    """

    return list(filter(None, l))


def DedupedList(l: list) -> list:
    """
    Убирает все дубликаты и None из списка

    Args:
        l (list): исходный список

    Returns:
        list: список без None и дубликатов
    """

    try:
        return list(set(NonNoneList(l)))

    except:
        # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
        return list({frozenset(item.items()): item for item in NonNoneList(l)}.values())


def MedianDedupedDF(df: pd.DataFrame, id_column_name: str, median_column_name: str) -> pd.DataFrame:
    """
    Удаляет дубликаты в колонке идентификаторов элементов DataFrame,
    заменяя их медианой соответствующих значений в колонке median_column_name.
    Сохраняет значения из всех остальных столбцов в списки, если они различны, иначе - одиночными элементами.

    Args:
        df (pd.DataFrame): исходный DataFrame
        id_column_name (str): имя колонки, в которой находятся идентификаторы
        median_column_name (str): имя колонки, в которой надо посчитать медианы

    Returns:
        pd.DataFrame: с удаленными дубликатами и списками в остальных столбцах.
    """

    median_and_id_data: dict = {}

    # значения в столбце, где будут медианы - должно быть типа float
    df[median_column_name] = df[median_column_name].astype(float)

    for name in df[id_column_name].unique():
        name_subset_df: pd.DataFrame = df.loc[df[id_column_name] == name]
        # создаем словарь для хранения данных по данному имени
        name_values_dict = {
            median_column_name: name_subset_df[median_column_name].median()}

        # добавляем списки значений для остальных столбцов
        for col in name_subset_df.columns:
            # исключаем колонку median_column_name
            if col != median_column_name and col != id_column_name:
                try:
                    name_values_dict[col] = DedupedList(
                        name_subset_df[col].tolist())

                except TypeError:
                    name_values_dict[col] = NonNoneList(
                        name_subset_df[col].tolist())

                # если в списке 1 элемент, то список бесполезен
                if len(name_values_dict[col]) == 1:
                    name_values_dict[col] = name_values_dict[col][0]

                def IsAllNan(l: list):
                    return all(str(elem) == "nan" for elem in l)

                # если в списке нет элементов, или они все == "nan", то это не список
                if (isinstance(name_values_dict[col], list)):
                    if len(name_values_dict[col]) == 0 or IsAllNan(name_values_dict[col]):
                        name_values_dict[col] = None

        # сохраняем данные для данного имени
        median_and_id_data[name] = name_values_dict

    # новый pd.DataFrame с уникальными значениями id_column_name и соответствующими данными
    new_df = pd.DataFrame.from_dict(
        median_and_id_data, orient='index').reset_index()
    new_df.rename(columns={'index': id_column_name}, inplace=True)

    return new_df
